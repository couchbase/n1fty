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
	"github.com/couchbase/query/value"
)

/*
  - 1st version can start by only supporting the default type mapping,
    ensuring that no other type mappings are defined.
  - 2nd version can also handle another case when default type mapping
    is disabled, and there is only 1 single explicit type mapping,
    with no disabled type mappings.
  - 3rd version can also handle another case when default type mapping
    is disabled, and there are only explicit type mappings,
    with no disabled type mappings.
  - 4th version can also handle another case when default type mapping
    is disabled, and there are only explicit type mappings,
    with no disabled type mappings.
  - 5th version can also handle another case when default type mapping
    exists and is enabled,
    along with explicit type mappings (perhaps some disabled).

disabled type mapping is similar to a non-dynamic type mapping with no
properties/fields defined -- nothing is in the index.

  - there's perhaps another variable in the matrix
    where dynamic indexing is either enabled/disabled.

in summary, when there's no dynamic indexing...
                  versions:
                            1      2      3      4      5
 - default mapping        : y                           y
 - 1 type mapping         :        1-only
 - N type mappings        :               y      y      y
 - disabled type mappings :                      y      y

dynamic indexing needs to check that all the analyzers are the same
across all mappings for a certain FieldPath, otherwise don't
expose the dynamic indexing.
- extra careful case is when some child FieldPath has inconsistent
  settings than some parental FieldPath.
- easy approach is all dynamic indexing must be the same, so no
  inconsistencies.

related, property/field usage has to also be the same across all mappings,
otherwise don't expose the property/field.

the main case...

need to check the WHERE clause for all type mappings
    (ex: type="beer"), because if you don't, there can be false negatives.
    - example: FTS index has type mapping where type="beer", but the N1QL is
      looking for WHERE name="coors"
      - beware the false negative since the FTS index will
        only have entries for beer docs and (importantly) will be missing
        entries for brewery docs whose brewery name is "coors".
    - to have no false negatives, the query has to look like
      WHERE type="beer" AND name="coors"
      otherwise n1fty should return not-sargable.
    - this should be done carefully on a per-conjunction level, a'la...
      ((type = "beer" AND
        beer_name = "coors" AND
        sargable-exprs-with-beer-only-fields AND
        any-other-fields-are-considered-filterable) OR
       (type = "brewery" AND
        brewery_name = coors" AND
        sargable-exprs-with-brewery-only-fields AND
        any-other-fields-are-considered-filterable)).
    - default type mapping need a more complex 'negative' type discriminator, like...
      ((type != "beer" AND type != "brewery") AND
       sargable-exprs-with-[non-beer/non-brewery]-fields AND
       any-other-fields-are-considered-filterable).
    - what if user references brewery fields (or other non-beer fields)
      in the beer-centric subtree?
      - ANS: those can be treated as filterable due to the conjunction,
             and even though n1fty might be returning additional
             false positives, it's logically correct.
  - an approach is to have a sequence of condition-to-FlexIndex pairings.
    - the doc type field can be checked as part of the conditions.
    - the doc type may be based on docId regexp or prefix delimiter.

// type=BEER
//   IndexedFields += "type"=>"string".
//   SupportedExprs += "eq"=>"type"=>"string"=>"ValueMust == BEER".
//     adds to fieldTracks but not to flexBuild (Effect: "FlexBuild:n").
//       ==> if only type field is in the query, then flexBuild will be empty,
//           leading (hopefully) to empty SearchResult.
//   will return not-sargable on any other usage of "type" field (via CheckFieldsUsed/IndexedFields).
//   NOTE: (AAA) this might produce false negatives during dynamic indexing,
//         which will activate, for example, on 'type >= "BEER"' supportedExpr
//         and incorrectly (?) add a cmpFieldConstant to the FlexBuild?
//         so, one solution is to add a high-precedence cmpFieldConstant
//         supportedExpr of "lt gt le ge" on the type field that always
//         returns not-sargable (via: Effect: "not-sargable").
//
// type=WINERY (disabled)
//   IndexedFields += "type"=>"string".
//   SupportedExprs += "eq"=>"type"=>"string"=>"ValueMust == WINERY"
//     ==> not-sargable (via: Effect: "not-sargable"), same as #AAA above.
//
// or, just using the type field in any way should be not-sargable instead of filterable.
//
// type=<DEFAULT>
//   IndexedFields += "type"=>"string".
//   SupportedExprs += "NEQ"=>"type"=>"string"=>["BEER", "WINERY"].
//     adds to fieldTracks but not to flexBuild (Effect: "FlexBuild:n").
//       ==> if only type field is in the query, then flexBuild will be empty.
//   will return not-sargable on any other usage of "type" field (via CheckFieldsUsed/IndexedFields).

/*
	// TODO: DocType might be specified via an id-regexp or prefix
	// delimiter, but here we only handle the type-field approach.
	CondFieldPath []string // Ex: ["type"], or empty when disabled.

	// The Cond might represent a simple condition like `type="BEER"`
	// or a complex negative check (e.g., !beer && !brewery) when the
	// default mapping is used along with >=1 explicit type mappings.
	// Might be empty on a disabled type mapping.
	CondFlexIndexes []*CondFlexIndex // Ex: [{type=BEER, &FlexIndex{}}].

// A negative list of non-matches for handling default mapping (!beer
// && !brewery) can be represented by the cfi.Cond.
//
// issue: Need to return not-sargable if any other type is referenced?
// ANS: - This is naturally taken care of during CheckFieldsUsed when
//        there's no dynamic indexing.
//      - Under dynamic indexing, need to add a SupportedExpr of
//        "Effect: not-sargable" to capture that "type" field is
//        never dynamically indexed?

TODO: support OR's of AND's.
TODO: support expressions that involve multiple types.
TODO: what if the type mapping also explicitly indexes the type field, too?
      - and, what if that explicit type field has a different analyzer?
*/

// -------------------------------------------------------------------------

// Associates a condition check to a FlexIndex, used for type mappings.
type CondFlexIndex struct {
	Cond      CondFunc
	FlexIndex *FlexIndex // Ex: the type mapping when type=BEER.
}

// A slice of conditional flex indexes.
type CondFlexIndexes []*CondFlexIndex

// Returns true when the expressions match the condition check.
type CondFunc func(ids Identifiers, es expression.Expressions) (bool, error)

// Returns a CondFunc that represents checking a field for value equality.
func MakeCondFuncEqVal(fieldPath []string, v value.Value) CondFunc {
	return func(ids Identifiers, es expression.Expressions) (bool, error) {
		for _, e := range es {
			f, ok := e.(*expression.Eq)
			if !ok {
				continue
			}

			matches, c := MatchEqFieldPathToConstant(ids, fieldPath, f)
			if matches && c.Value().Equals(v).Truth() {
				return true, nil
			}
		}

		return false, nil
	}
}

// Returns a CondFunc that represents checking a field is not equal to
// any of the given vals.
func MakeCondFuncNeqVals(fieldPath []string, vals []string) CondFunc {
	return func(ids Identifiers, es expression.Expressions) (bool, error) {
		if len(vals) <= 0 {
			return true, nil // Empty vals means CondFunc always matches.
		}

		m := map[value.Value]struct{}{}
		for _, v := range vals {
			m[value.NewValue(v)] = struct{}{}
		}

		for _, e := range es {
			n, ok := e.(*expression.Not)
			if !ok {
				continue
			}

			f, ok := n.Operand().(*expression.Eq)
			if !ok {
				continue
			}

			matches, c := MatchEqFieldPathToConstant(ids, fieldPath, f)
			if matches {
				delete(m, c.Value())
			}
		}

		return len(m) <= 0, nil
	}
}

// -------------------------------------------------------------------------

func MatchEqFieldPathToConstant(ids Identifiers, fieldPath []string,
	f *expression.Eq) (bool, *expression.Constant) {
	m := func(first, second expression.Expression) (bool, *expression.Constant) {
		suffix, ok := ExpressionFieldPathSuffix(ids, first, fieldPath, nil)
		if ok && len(suffix) <= 0 {
			if c, ok := second.(*expression.Constant); ok {
				return true, c
			}
		}

		return false, nil
	}

	matches, c := m(f.First(), f.Second()) // Check pattern a.foo = c.
	if matches {
		return matches, c
	}

	return m(f.Second(), f.First()) // Commute to check c = a.foo.
}

// -------------------------------------------------------------------------

func (s CondFlexIndexes) Sargable(
	ids Identifiers, e expression.Expression, eFTs FieldTypes) (
	rFieldTracks FieldTracks, rNeedsFiltering bool, rFB *FlexBuild, err error) {
	o, ok := e.(*expression.Or)
	if !ok {
		o = expression.NewOr(e)
	}

	// OR-of-AND's means all the OR's children must match a flex index.
	for _, oChild := range o.Children() {
		cFI, err := s.FindFlexIndex(ids, oChild)
		if err != nil || cFI == nil {
			return nil, false, nil, err
		}

		cFieldTracks, cNeedsFiltering, cFB, err := cFI.Sargable(ids, oChild, eFTs)
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
				rFB = &FlexBuild{Kind: "disjunct"}
			}
			rFB.Children = append(rFB.Children, cFB)
		}

		if len(cFieldTracks) > 0 {
			continue // Expression sargable.
		}

		return nil, false, nil, nil // Expression not-sargable.
	}

	if rFB != nil && len(rFB.Children) == 1 { // Optimize OR-of-1-expr.
		return rFieldTracks, rNeedsFiltering, rFB.Children[0], nil
	}

	// All expressions in the OR were sargable.
	return rFieldTracks, rNeedsFiltering, rFB, nil
}

// Find the first FlexIndex whose Cond matches the given AND expression.
func (s CondFlexIndexes) FindFlexIndex(ids Identifiers, e expression.Expression) (
	rv *FlexIndex, err error) {
	var children []expression.Expression
	var collectExpressions func(ex expression.Expression)
	collectExpressions = func(ex expression.Expression) {
		if exAnd, ok := ex.(*expression.And); ok {
			for _, child := range exAnd.Children() {
				collectExpressions(child)
			}
		} else {
			children = append(children, ex)
		}
	}

	collectExpressions(e)

	for _, cfi := range s {
		matches, err := cfi.Cond(ids, children)
		if err != nil {
			return nil, err
		}

		if matches {
			if rv != nil {
				return nil, nil // >1 matches, so not a match.
			}

			rv = cfi.FlexIndex
		}
	}

	return rv, nil // Might return nil.
}
