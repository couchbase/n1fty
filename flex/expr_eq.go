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
	"strings"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
)

// SupportedExprEqFieldConstant implements the SupportedExpr interface
// and represents an 'eq' comparison of a field to a constant value
// with a given type.
type SupportedExprEqFieldConstant struct {
	// Regular examples: ["name"], ["description"], ["address", "city"].
	// Dynamic examples: [], ["address"].
	FieldPath []string

	ValueType string // Ex: "string", "number", etc.

	// When FieldPathPartial is true, FieldPath represents a prefix or
	// leading part of a full field path, used for dynamic indexing.
	FieldPathPartial bool
}

func (s *SupportedExprEqFieldConstant) Supports(
	fi *FlexIndex, identifiers Identifiers,
	expr expression.Expression, exprFieldTypes FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	f, ok := expr.(expression.BinaryFunction)
	if !ok || f.Name() != "eq" {
		return false, nil, false, nil, nil
	}

	matches, fieldTracks, needsFiltering, flexBuild, err :=
		s.supportsXY(fi, identifiers, f.First(), f.Second(), exprFieldTypes)
	if err != nil || matches {
		return matches, fieldTracks, needsFiltering, flexBuild, err
	}

	// Check reverse, since "x = y" is commutative with "y = x".
	return s.supportsXY(fi, identifiers, f.Second(), f.First(), exprFieldTypes)
}

func (s *SupportedExprEqFieldConstant) supportsXY(
	fi *FlexIndex, identifiers Identifiers,
	exprX, exprY expression.Expression, exprFieldTypes FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	suffix, ok := ExpressionFieldPathSuffix(identifiers, exprX,
		s.FieldPath, nil)
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

	fieldTrack := strings.Join(s.FieldPath, ".")
	if len(suffix) > 0 {
		if len(fieldTrack) > 0 {
			fieldTrack = fieldTrack + "."
		}
		fieldTrack = fieldTrack + strings.Join(suffix, ".")
	}

	switch x := exprY.(type) {
	case *expression.Constant:
		if x.Type().String() != s.ValueType {
			return true, nil, false, nil, nil // Wrong const type, so not-sargable.
		}

	case *algebra.NamedParameter:
		fieldType, ok := exprFieldTypes.Lookup(FieldTrack(fieldTrack))
		if !ok || fieldType != s.ValueType {
			return true, nil, false, nil, nil // Wrong param type, so not-sargable.
		}

	case *algebra.PositionalParameter:
		fieldType, ok := exprFieldTypes.Lookup(FieldTrack(fieldTrack))
		if !ok || fieldType != s.ValueType {
			return true, nil, false, nil, nil // Wrong param type, so not-sargable.
		}

	default: // Not a constant or parameter, etc, so not-sargable.
		return true, nil, false, nil, nil
	}

	return true, FieldTracks{FieldTrack(fieldTrack): 1}, false, &FlexBuild{
		Kind: "expr",
		Data: []string{"eqFieldConstant", fieldTrack, s.ValueType, exprY.String()},
	}, nil
}
