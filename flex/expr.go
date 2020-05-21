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
	"fmt"
	"strings"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// Allows apps to declare supported expressions to FlexSargable().
type SupportedExpr interface {
	// Checks whether the SupportedExpr can handle an expr.
	Supports(fi *FlexIndex, ids Identifiers,
		expr expression.Expression, exprFTs FieldTypes) (matches bool,
		ft FieldTracks, needsFiltering bool, fb *FlexBuild, err error)
}

// ----------------------------------------------------------------------

// A supported expression that never matches, useful for debugging.
type SupportedExprNoop struct{}

func (s *SupportedExprNoop) Supports(fi *FlexIndex, ids Identifiers,
	expr expression.Expression, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	fmt.Printf("SupportedExprNoop, expr: %+v, %#v\n", expr, expr)
	return false, nil, false, nil, nil
}

// ----------------------------------------------------------------------

// A supported expression that always returns not-sargable, useful to
// mark unknown exprs as not-sargable rather than as filterable.
type SupportedExprNotSargable struct{}

func (s *SupportedExprNotSargable) Supports(fi *FlexIndex, ids Identifiers,
	expr expression.Expression, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	return true, nil, false, nil, nil
}

// ----------------------------------------------------------------------

// SupportedExprCmpFieldConstant implements the SupportedExpr
// interface and represents a comparison of a field to a constant
// value with a given type.
type SupportedExprCmpFieldConstant struct {
	Cmp string // Ex: "eq" (default when ""), "lt", "le", "lt le", etc.

	// Regular examples: ["description"], ["address", "city"].
	// Dynamic examples: [], ["address"].
	FieldPath []string

	ValueType string // Ex: "text", "number", etc.

	// When non-nil, constant must equal this value, else not-sargable.
	ValueMust value.Value

	// When FieldPathPartial is true, FieldPath represents a prefix or
	// leading part of a full field path, used for dynamic indexing.
	FieldPathPartial bool

	// When FieldTypeCheck is true, additional type checks on the
	// FieldPath are done based on field types that were learned from
	// the expr (e.g., ISSTRING(a), ISNUMBER(a)).
	FieldTypeCheck bool

	// Advanced control of output effect on FieldTracks / FlexBuild.
	// Ex: "" (default output), "not-sargable", "FlexBuild:n".
	Effect string
}

var CmpReverse = map[string]string{
	"eq": "eq", "lt": "gt", "le": "ge", "gt": "lt", "ge": "le",
}

func (s *SupportedExprCmpFieldConstant) Supports(fi *FlexIndex, ids Identifiers,
	expr expression.Expression, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	f, ok := expr.(expression.BinaryFunction)
	if !ok || (s.Cmp == "" && f.Name() != "eq") ||
		(s.Cmp != "" && !strings.Contains(s.Cmp, f.Name())) {
		return false, nil, false, nil, nil
	}

	matches, fieldTracks, needsFiltering, flexBuild, err :=
		s.SupportsXY(fi, ids, f.Name(), f.First(), f.Second(), exprFTs)
	if err != nil || matches {
		return matches, fieldTracks, needsFiltering, flexBuild, err
	}

	return s.SupportsXY(fi, ids, CmpReverse[f.Name()], f.Second(), f.First(), exprFTs)
}

func (s *SupportedExprCmpFieldConstant) SupportsXY(fi *FlexIndex, ids Identifiers,
	fName string, exprX, exprY expression.Expression, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	suffix, ok := ExpressionFieldPathSuffix(ids, exprX, s.FieldPath, nil)
	if !ok {
		return false, nil, false, nil, nil
	}

	if s.FieldPathPartial != (len(suffix) > 0) {
		return false, nil, false, nil, nil
	}

	for _, s := range suffix {
		if s == "" { // Complex or array element is not-sargable.
			return true, nil, false, nil, nil
		}
	}

	exprFieldTypesCheck := s.FieldTypeCheck

	switch x := exprY.(type) {
	case *expression.Constant:
		xType := x.Type().String()
		if xType == "string" {
			xType = "text"
			// Check if the search term could be datetime (ISO-8601)
			if exprYVal := exprY.Value(); exprYVal != nil {
				if v, ok := exprYVal.Actual().(string); ok {
					if _, _, err := expression.StrToTimeFormat(v); err == nil {
						xType = "datetime"
					}
				}
			}
		}

		if fi.Dynamic {
			if xType != s.ValueType {
				// If the flex index is dynamic, there'd exist IndexedFields
				// with all data types, so continue search for IndexedFields
				// where the type matches.
				return false, nil, false, nil, nil
			}
		} else {
			if xType != s.ValueType {
				if s.ValueType == "text" && xType == "datetime" {
					// In a non-dynamic index, allow the case where the stored
					// indexed field is of type "text", and the query over the
					// field contains a string but of a datetime format,
					// in which case a term search will ensue over the field.
					break
				}
				// Wrong type, so not-sargable, but can be filtered out
				return true, nil, true, nil, nil
			}
		}

	case *expression.Neg:
		// type : "number"
		if fi.Dynamic {
			if x.Type().String() != s.ValueType {
				// If the flex index is dynamic, there'd exist IndexedFields
				// with all data types, so continue search for IndexedFields
				// where the type matches.
				return false, nil, false, nil, nil
			}
		} else if x.Type().String() != BleveTypeConv[s.ValueType] {
			// Wrong type, so not-sargable, but can be filtered out
			return true, nil, true, nil, nil
		}

	case *algebra.NamedParameter:
		exprFieldTypesCheck = true

	case *algebra.PositionalParameter:
		exprFieldTypesCheck = true

	default: // Not a constant or parameter, etc, so not-sargable.
		return true, nil, false, nil, nil
	}

	fieldTrack := strings.Join(s.FieldPath, ".")
	if len(suffix) > 0 {
		if len(fieldTrack) > 0 {
			fieldTrack = fieldTrack + "."
		}
		fieldTrack = fieldTrack + strings.Join(suffix, ".")
	}

	if exprFieldTypesCheck {
		fieldType, ok := exprFTs.Lookup(FieldTrack(fieldTrack))
		if !ok || fieldType != BleveTypeConv[s.ValueType] {
			// Wrong field type, not-sargable (check filtering, for conjuncts)
			return true, nil, true, nil, nil
		}
	}

	if s.ValueMust != nil { // Must be a constant equal to ValueMust.
		if c, ok := exprY.(*expression.Constant); !ok ||
			!s.ValueMust.Equals(c.Value()).Truth() {
			// If the flex index were to have multiple type mappings, there'd
			// exist multiple supported expressions for evaluating the condition
			// expression, so return matches=false to continue testing for
			// supportability within another expression.
			return false, nil, false, nil, nil
		}
	}

	if strings.Contains(s.Effect, "not-sargable") {
		return true, nil, false, nil, nil
	}

	if strings.Contains(s.Effect, "FlexBuild:n") {
		return true, FieldTracks{FieldTrack(fieldTrack): 1}, false, nil, nil
	}

	return true, FieldTracks{FieldTrack(fieldTrack): 1}, false, &FlexBuild{
		Kind: "cmpFieldConstant", Data: []string{
			fName, fieldTrack, s.ValueType, exprY.String(),
		}}, nil
}
