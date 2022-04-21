//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package flex

import (
	"encoding/json"

	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/search"
)

// FlexIndex represents the subset of a flexible index definition
// that's needed for sargable processing.  It's immutable, so it's
// safe for use with multiple, concurrent Sargable() calls.
//
// FlexIndex's Dynamic is set to true if the index definition is
// default dynamic with default_analyzer: keyword, in which case
// SupportedExprs for multiple types will be made available.
type FlexIndex struct {
	Name             string
	UUID             string
	IndexedFields    FieldInfos // Ex: "hireDate", "city", "salary".
	SortableFields   []string
	SupportedExprs   []SupportedExpr
	FieldTrackTypes  map[FieldTrack]map[string]struct{} // Ex: {"city":{{"airport":..}}}
	MultipleTypeStrs bool
	Dynamic          bool
	StoreDynamic     bool
	DocValuesDynamic bool
}

// This API interprets a search expression for the FlexIndex.
func (fi *FlexIndex) interpretSearchFunc(s *search.Search) (
	bool, FieldTracks, *FlexBuild) {
	if fi.MultipleTypeStrs {
		// Do NOT interpret a SEARCH(..) function for an FTS index that
		// has multiple type mappings (to avoid possibility of false negatives).
		return false, nil, nil
	}

	// Firstly - validate options provided within the Search expression.
	if s.Options() != nil {
		if optionsVal := s.Options().Value(); optionsVal != nil {
			for k, v := range optionsVal.Fields() {
				if k == "index" {
					vStr, ok := v.(string)
					// only string value for "index" supported for push down
					if !ok || fi.Name != vStr {
						return false, nil, nil
					}
				} else if k == "indexUUID" {
					vStr, ok := v.(string)
					// "indexUUID" to be a string
					if !ok || fi.UUID != vStr {
						return false, nil, nil
					}
				} else {
					// if other options present, do not push down SEARCH(..)
					return false, nil, nil
				}
			}
		}
	}

	// Next - look into the Query part of the search expression.
	if s.Query() == nil || s.Query().Value() == nil {
		return false, nil, nil
	}

	queryVal := s.Query().Value()
	field := util.CleanseField(s.FieldName())
	if srq, ok := queryVal.Field("query"); ok {
		// This is a bleve SearchRequest.
		// Continue only if it doesn't carry any other settings.
		// This is so FLEX would not have to handle various pagination
		// and timeout settings that could be provided within the
		// SEARCH function for only a part of the query.

		if len(queryVal.Fields()) > 1 {
			return false, nil, nil
		}

		queryVal = srq
	}

	q, err := util.BuildQuery(field, queryVal)
	if err != nil {
		return false, nil, nil
	}

	queryFields, err := util.FetchFieldsToSearchFromQuery(q)
	if err != nil {
		return false, nil, nil
	}

	qBytes, _ := json.Marshal(q)
	var qInterface map[string]interface{}
	json.Unmarshal(qBytes, &qInterface)

	fieldTracks := FieldTracks{}
	for f := range queryFields {
		// Check if the query field is either indexed or the flex index
		// is dynamic for the query to be sargable.
		if !fi.IndexedFields.Contains(f.Name, f.Type, f.Analyzer) &&
			!fi.Dynamic {
			return false, nil, nil
		}
	}

	// Search expression supported only if all fields requested
	// for are indexed within the FTS index.
	fieldTracks[FieldTrack(s.String())] = 1
	return true, fieldTracks, &FlexBuild{
		Kind: "searchQuery",
		Data: qInterface,
	}
}

// Sargable() checks if expression (e) is amenable to a FlexIndex scan.
//
// When len(returned FieldTracks) > 0, then the expr tree is sargable,
// where the associated per-fieldTrack counts provide more resolution.
//
// The returned bool indicates potential false positives.
//
// The returned FlexBuild represents hierarchical, gathered state that
// can be used to formulate index scans and is meant to help avoid
// re-examinations of the expr.
//
// The FieldTypes (eFTs) represents field-type's info about the expr
// learned during recursion, and can be nil for the initial call.
//
// The algorithm tries to recursively find a subset of the expr tree
// where sub-expressions are either allowed by the SupportedExprs, or
// are intermediary AND/OR composite expressions, or are expressions
// that are filterable later for potential false-positives.
func (fi *FlexIndex) Sargable(ids Identifiers, e expression.Expression,
	requestedTypes []string, eFTs FieldTypes) (
	FieldTracks, bool, *FlexBuild, error) {
	// Check if the expression is a Search expression.
	if searchFunc, ok := e.(*search.Search); ok {
		if canDo, ft, fb := fi.interpretSearchFunc(searchFunc); canDo {
			return ft, false, fb, nil
		}
	}

	// Check if matches one of the supported expressions.
	for _, se := range fi.SupportedExprs {
		matches, ft, needsFiltering, fb, err := se.Supports(fi, ids, e, requestedTypes, eFTs)
		if err != nil || matches {
			return ft, needsFiltering, fb, err
		}
	}

	// When not an explicitly supported expr, it might be a combination expr.
	return fi.SargableCombo(ids, e, requestedTypes, eFTs)
}

func (fi *FlexIndex) SargableCombo(ids Identifiers, e expression.Expression,
	requestedTypes []string, eFTs FieldTypes) (FieldTracks, bool, *FlexBuild, error) {
	if _, ok := e.(*expression.And); ok { // Handle AND composite.
		return fi.SargableComposite(ids, collectConjunctExprs(e, nil), requestedTypes, eFTs, "conjunct")
	}

	if _, ok := e.(*expression.Or); ok { // Handle OR composite.
		return fi.SargableComposite(ids, collectDisjunctExprs(e, nil), requestedTypes, eFTs, "disjunct")
	}

	if a, ok := e.(*expression.Any); ok { // Handle ANY-SATISFIES.
		return fi.SargableAnySatisfies(ids, a, requestedTypes, eFTs, false)
	}

	if a, ok := e.(*expression.AnyEvery); ok { // ANY-AND-EVERY-SATISFIES.
		return fi.SargableAnySatisfies(ids, a, requestedTypes, eFTs, true)
	}

	// Otherwise, any other expr that references or uses any of our
	// indexedFields (in a non-supported way) is not-sargable.
	// Ex: ROUND(myIndexedField) > 100.
	// (can be filtered out though)
	if used, err := CheckFieldsUsed(fi.IndexedFields, ids, e); err != nil || used {
		return nil, true, nil, err
	}

	// Otherwise, any other expression is filterable as a "false
	// positive" case.  Ex: fieldWeDontCareAbout > 100.
	return nil, true, nil, nil
}

// ---------------------------------------------------------------

// Processes a composite expression (AND's/OR's) via recursion.
func (fi *FlexIndex) SargableComposite(ids Identifiers,
	es expression.Expressions, requestedTypes []string, eFTs FieldTypes, kind string) (
	rFieldTracks FieldTracks, rNeedsFiltering bool, rFB *FlexBuild, err error) {
	conjunct := kind == "conjunct"
	if conjunct {
		// A conjunct allows us to build up field-type knowledge from
		// the expressions, possibly with filtering out some of them.
		var ok bool
		es, eFTs, ok = LearnConjunctFieldTypes(fi.IndexedFields, ids, es, eFTs)
		if !ok {
			return nil, true, nil, nil // Type mismatch is not-sargable.
		}
	}

	// Loop through the expressions and recurse.  Return early if we
	// find an expression that's not-sargable or isn't filterable.
	for _, cExpr := range es {
		cFieldTracks, cNeedsFiltering, cFB, err := fi.Sargable(ids, cExpr, requestedTypes, eFTs)
		if err != nil {
			return nil, false, nil, err
		}

		// Add the recursion's result to our composite results.
		for cFieldTrack, n := range cFieldTracks {
			if rFieldTracks == nil {
				rFieldTracks = FieldTracks{}
			}
			rFieldTracks[cFieldTrack] += n
		}

		rNeedsFiltering = rNeedsFiltering || cNeedsFiltering

		if cFB != nil {
			if rFB == nil {
				rFB = &FlexBuild{Kind: kind}
			}
			rFB.Children = append(rFB.Children, cFB)
		}

		if len(cFieldTracks) > 0 {
			continue // Expression sargable.
		}

		if cNeedsFiltering && conjunct {
			continue // Expression is filterable when conjunct'ing.
		}

		return nil, true, nil, nil // Expression not-sargable, can be filtered out
	}

	// All expressions were sargable or ok for filtering.
	return rFieldTracks, rNeedsFiltering, rFB, nil
}

// ---------------------------------------------------------------

// Process ANY-(AND-EVERY)-SATISFIES by pushing onto identifiers stack.
func (fi *FlexIndex) SargableAnySatisfies(ids Identifiers,
	a expression.CollPredicate, requestedTypes []string, ftypes FieldTypes,
	forceNeedsFiltering bool) (FieldTracks, bool, *FlexBuild, error) {
	// For now, only 1 binding, as chain semantics is underspecified.
	ids, ok := ids.Push(a.Bindings(), 1)
	if !ok {
		return nil, false, nil, nil
	}

	e := a.Satisfies()
	if !forceNeedsFiltering {
		// In case of ANY-SATISFIES, if in case of multiple predicates -
		// force filtering as FTS does not track index position within an array.

		switch e.(type) {
		case *expression.And:
			forceNeedsFiltering = true
		case *expression.Or:
			forceNeedsFiltering = true
		case *expression.Any:
			forceNeedsFiltering = true
		case *expression.AnyEvery:
			forceNeedsFiltering = true
		default:
		}
	}

	ft, needsFiltering, fb, err := fi.Sargable(ids, e, requestedTypes, ftypes)

	return ft, needsFiltering || forceNeedsFiltering, fb, err
}

// ----------------------------------------------------------------

// FlexBuild represents hierarchical state that's gathered during
// Sargable() recursion, which can be used to formulate an index scan.
type FlexBuild struct {
	Kind     string
	Children []*FlexBuild
	Data     interface{} // Depends on the kind.
}
