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
	"github.com/couchbase/query/expression"
)

// FlexIndex represents the subset of a flexible index definition
// that's needed for sargable processing.  It's immutable, so it's
// safe for use with multiple, concurrent Sargable() calls.
type FlexIndex struct {
	IndexedFields  FieldInfos // Ex: "hireDate", "city", "salary".
	SupportedExprs []SupportedExpr
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
	eFTs FieldTypes) (FieldTracks, bool, *FlexBuild, error) {
	// Check if expr matches one of the supported expressions.
	for _, se := range fi.SupportedExprs {
		matches, ft, needsFiltering, fb, err := se.Supports(fi, ids, e, eFTs)
		if err != nil || matches {
			return ft, needsFiltering, fb, err
		}
	}

	if _, ok := e.(*expression.And); ok { // Handle AND composite.
		return fi.SargableComposite(ids, e.Children(), eFTs, "conjunct")
	}

	if _, ok := e.(*expression.Or); ok { // Handle OR composite.
		return fi.SargableComposite(ids, e.Children(), eFTs, "disjunct")
	}

	if a, ok := e.(*expression.Any); ok { // Handle ANY-SATISFIES.
		return fi.SargableAnySatisfies(ids, a, eFTs, false)
	}

	if a, ok := e.(*expression.AnyEvery); ok { // ANY-AND-EVERY-SATISFIES.
		return fi.SargableAnySatisfies(ids, a, eFTs, true)
	}

	// Otherwise, any other expr that references or uses any of our
	// indexedFields (in a non-supported way) is not-sargable.
	// Ex: ROUND(myIndexedField) > 100.
	if used, err := CheckFieldsUsed(fi.IndexedFields, ids, e); err != nil || used {
		return nil, false, nil, err
	}

	// Otherwise, any other expression is filterable as a "false
	// positive" case.  Ex: fieldWeDontCareAbout > 100.
	return nil, true, nil, nil
}

// ---------------------------------------------------------------

// Processes a composite expression (AND's/OR's) via recursion.
func (fi *FlexIndex) SargableComposite(ids Identifiers,
	es expression.Expressions, eFTs FieldTypes, kind string) (
	rFieldTracks FieldTracks, rNeedsFiltering bool, rFB *FlexBuild, err error) {
	conjunct := kind == "conjunct"
	if conjunct {
		// A conjunct allows us to build up field-type knowledge from
		// the expressions, possibly with filtering out some of them.
		var ok bool
		es, eFTs, ok = LearnConjunctFieldTypes(fi.IndexedFields, ids, es, eFTs)
		if !ok {
			return nil, false, nil, nil // Type mismatch is not-sargable.
		}
	}

	// Loop through the expressions and recurse.  Return early if we
	// find an expression that's not-sargable or isn't filterable.
	for _, cExpr := range es {
		cFieldTracks, cNeedsFiltering, cFB, err := fi.Sargable(ids, cExpr, eFTs)
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

		return nil, false, nil, nil // Expression not-sargable.
	}

	// All expressions were sargable or ok for filtering.
	return rFieldTracks, rNeedsFiltering, rFB, nil
}

// ---------------------------------------------------------------

// Process ANY-(AND-EVERY)-SATISFIES by pushing onto identifiers stack.
func (fi *FlexIndex) SargableAnySatisfies(ids Identifiers,
	a expression.CollectionPredicate, ftypes FieldTypes,
	forceNeedsFiltering bool) (FieldTracks, bool, *FlexBuild, error) {
	// For now, only 1 binding, as chain semantics is underspecified.
	ids, ok := ids.Push(a.Bindings(), 1)
	if !ok {
		return nil, false, nil, nil
	}

	ft, needsFiltering, fb, err := fi.Sargable(ids, a.Satisfies(), ftypes)

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
