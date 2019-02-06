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

// Sargable() checks if an expr is amenable to a FlexIndex scan.
//
// When len(returned fieldTracks) > 0, then the expr tree is sargable,
// where the associated per-fieldTrack counts provide more resolution.
//
// The returned needsFiltering indicates potential false positives.
//
// The returned flexBuild represents hierarchical, gathered state that
// can be used to formulate index scans and is meant to help avoid
// re-examinations of the expr.
//
// The exprFieldTypes represents field-type knowledge that's learned
// during the recursion, and can be nil for the initial call.
//
// The algorithm tries to recursively find a subset of the expr tree
// where sub-expressions are either allowed by the SupportedExprs, or
// are intermediary AND/OR composite expressions, or are expressions
// that are filterable later for potential false-positives.
func (fi *FlexIndex) Sargable(identifiers Identifiers,
	expr expression.Expression, exprFieldTypes FieldTypes) (
	fieldTracks FieldTracks, needsFiltering bool,
	flexBuild *FlexBuild, err error) {
	// Check if expr matches one of the supported expressions.
	for _, supportedExpr := range fi.SupportedExprs {
		var matches bool
		matches, fieldTracks, needsFiltering, flexBuild, err =
			supportedExpr.Supports(fi, identifiers, expr, exprFieldTypes)
		if err != nil || matches {
			return fieldTracks, needsFiltering, flexBuild, err
		}
	}

	if _, ok := expr.(*expression.And); ok { // Handle AND composite.
		return fi.SargableComposite(identifiers, expr, exprFieldTypes, "conjunct")
	}

	if _, ok := expr.(*expression.Or); ok { // Handle OR composite.
		return fi.SargableComposite(identifiers, expr, exprFieldTypes, "disjunct")
	}

	if a, ok := expr.(*expression.Any); ok { // Handle ANY-SATISFIES.
		return fi.SargableAnySatisfies(identifiers, a, exprFieldTypes, false)
	}

	if a, ok := expr.(*expression.AnyEvery); ok { // ANY-AND-EVERY-SATISFIES.
		return fi.SargableAnySatisfies(identifiers, a, exprFieldTypes, true)
	}

	// Otherwise, any other expr that references or uses any of our
	// indexedFields (in a non-supported way) is not-sargable.
	// Ex: ROUND(myIndexedField) > 100.
	found, err := CheckFieldsUsed(fi.IndexedFields, identifiers, expr)
	if err != nil || found {
		return nil, false, nil, err
	}

	// Otherwise, any other expression is filterable as a "false
	// positive" case.  Ex: fieldWeDontCareAbout > 100.
	return nil, true, nil, nil
}

// ---------------------------------------------------------------

// Processes a composite expression (AND's/OR's) via recursion.
func (fi *FlexIndex) SargableComposite(identifiers Identifiers,
	expr expression.Expression, exprFieldTypes FieldTypes, kind string) (
	resFieldTracks FieldTracks, resNeedsFiltering bool,
	resFlexBuild *FlexBuild, err error) {
	exprs := expr.Children()

	conjunct := kind == "conjunct"
	if conjunct {
		// A conjunct allows us to build up field-type knowledge from
		// the children, possibly with filtering out some children.
		var ok bool
		exprs, exprFieldTypes, resNeedsFiltering, ok =
			ProcessConjunctFieldTypes(fi.IndexedFields,
				identifiers, exprs, exprFieldTypes)
		if !ok {
			return nil, false, nil, nil // Type mismatch is not-sargable.
		}
	}

	// Loop through the child exprs and recurse.  Return early if we
	// find a child expr that's not-sargable or that isn't filterable.
	for _, childExpr := range exprs {
		childFieldTracks, childNeedsFiltering, childFlexBuild, err :=
			fi.Sargable(identifiers, childExpr, exprFieldTypes)
		if err != nil {
			return nil, false, nil, err
		}

		// Add child's result to our composite results.
		for childFieldTrack, n := range childFieldTracks {
			if resFieldTracks == nil {
				resFieldTracks = FieldTracks{}
			}
			resFieldTracks[childFieldTrack] += n
		}

		resNeedsFiltering = resNeedsFiltering || childNeedsFiltering

		if childFlexBuild != nil {
			if resFlexBuild == nil {
				resFlexBuild = &FlexBuild{Kind: kind}
			}
			resFlexBuild.Children =
				append(resFlexBuild.Children, childFlexBuild)
		}

		if len(childFieldTracks) > 0 {
			continue // Child is sargable.
		}

		if childNeedsFiltering && conjunct {
			continue // Child ok for filtering when conjunct'ing.
		}

		return nil, false, nil, nil // We found a not-sargable child.
	}

	// All our children were sargable and/or ok for filtering.
	return resFieldTracks, resNeedsFiltering, resFlexBuild, nil
}

// ---------------------------------------------------------------

// Process ANY-(AND-EVERY)-SATISFIES by pushing onto identifiers stack.
func (fi *FlexIndex) SargableAnySatisfies(ids Identifiers,
	a expression.CollectionPredicate, ftypes FieldTypes,
	forceNeedsFiltering bool) (FieldTracks, bool, *FlexBuild, error) {
	// For now, only 1 binding, as chain semantics is underspecified.
	ids, ok := ids.PushBindings(a.Bindings(), 1)
	if !ok {
		return nil, false, nil, nil
	}

	ftracks, needsFiltering, fb, err := fi.Sargable(ids, a.Satisfies(), ftypes)

	return ftracks, needsFiltering || forceNeedsFiltering, fb, err
}

// ----------------------------------------------------------------

// FlexBuild represents hierarchical state that's gathered during
// Sargable() recursion, which can be used to formulate an index scan.
type FlexBuild struct {
	Kind     string
	Children []*FlexBuild
	Data     interface{} // Depends on the kind.
}
