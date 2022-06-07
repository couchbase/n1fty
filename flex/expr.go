//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
	Supports(fi *FlexIndex, ids Identifiers, expr expression.Expression,
		requestedTypes []string, exprFTs FieldTypes) (matches bool,
		ft FieldTracks, needsFiltering bool, fb *FlexBuild, err error)
}

// ----------------------------------------------------------------------

// A supported expression that never matches, useful for debugging.
type SupportedExprNoop struct{}

func (s *SupportedExprNoop) Supports(fi *FlexIndex, ids Identifiers,
	expr expression.Expression, requestedTypes []string, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	fmt.Printf("SupportedExprNoop, expr: %+v, %#v\n", expr, expr)
	return false, nil, false, nil, nil
}

// ----------------------------------------------------------------------

// A supported expression that always returns not-sargable, useful to
// mark unknown exprs as not-sargable rather than as filterable.
type SupportedExprNotSargable struct{}

func (s *SupportedExprNotSargable) Supports(fi *FlexIndex, ids Identifiers,
	expr expression.Expression, requestedTypes []string, exprFTs FieldTypes) (
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
	expr expression.Expression, requestedTypes []string, exprFTs FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	f, ok := expr.(expression.BinaryFunction)
	if !ok || (s.Cmp == "" && f.Name() != "eq") ||
		(s.Cmp != "" && !strings.Contains(s.Cmp, f.Name())) {
		return false, nil, false, nil, nil
	}

	matches, fieldTracks, needsFiltering, flexBuild, err :=
		s.SupportsXY(fi, ids, f.Name(), f.First(), f.Second(),
			requestedTypes, exprFTs)
	if err != nil || matches {
		return matches, fieldTracks, needsFiltering, flexBuild, err
	}

	return s.SupportsXY(fi, ids, CmpReverse[f.Name()], f.Second(), f.First(),
		requestedTypes, exprFTs)
}

func (s *SupportedExprCmpFieldConstant) SupportsXY(fi *FlexIndex, ids Identifiers,
	fName string, exprX, exprY expression.Expression, requestedTypes []string,
	exprFTs FieldTypes) (bool, FieldTracks, bool, *FlexBuild, error) {
	var suffix []string
	if ok := isEquivalentMetaIDExpr(exprX, s.FieldPath); !ok {
		suffix, ok = ExpressionFieldPathSuffix(ids, exprX, s.FieldPath, nil)
		if !ok {
			return false, nil, false, nil, nil
		}
	}

	if s.FieldPathPartial != (len(suffix) > 0) {
		return false, nil, false, nil, nil
	}

	for _, suf := range suffix {
		if suf == "" { // Complex or array element is not-sargable.
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
				return false, nil, true, nil, nil
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
			return false, nil, true, nil, nil
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

	if len(requestedTypes) > 0 {
		if supportedTypes, exists := fi.FieldTrackTypes[FieldTrack(fieldTrack)]; exists {
			for _, typ := range requestedTypes {
				if _, exists := supportedTypes[typ]; !exists {
					// One of the requested types isn't supported for this field path
					return false, nil, true, nil, nil
				}
			}
		}
	}

	if exprFieldTypesCheck {
		fieldType, ok := exprFTs.Lookup(FieldTrack(fieldTrack))
		if !ok || fieldType != BleveTypeConv[s.ValueType] {
			// Wrong field type, not-sargable (check filtering, for conjuncts)
			return false, nil, true, nil, nil
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

// This function establishes the equivalence of:
// meta().`id` and meta(`keyspace-alias`).`id`.
//
// p.s. The field path for docid_prefix and docid_regexp indexes is
// set to meta().`id`.
func isEquivalentMetaIDExpr(expr expression.Expression, fieldPath []string) bool {
	if f, ok := expr.(*expression.Field); ok {
		if m, ok := f.First().(*expression.Meta); ok {
			if f.Second() != nil {
				metaIdExpr := m.Name() + "()." + f.Second().String()
				for _, p := range fieldPath {
					if p == metaIdExpr {
						return true
					}
				}
			}
		}
	}

	return false
}
