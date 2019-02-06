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

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// A copy-on-write, left-side-pushed stack of immutable dictionaries.
type FieldTypes []map[FieldTrack]string

func (f FieldTypes) Lookup(k FieldTrack) (string, bool) {
	for _, m := range f {
		v, ok := m[k]
		if ok {
			return v, true
		}
	}

	return "", false
}

// ---------------------------------------------------------------

// ProcessConjunctFieldTypes examines the conjunct exprs (e.g.,
// children of an "AND" expression) for field-type information.  Only
// the expressions related to the given identifiers and
// indexedFields are considered.  It returns a (potentially
// filtered/simplified) copy of the exprs, where the
// filtering/simplification happens as field-type's are learned.
//
// The learned field-type information is pushed in immutable,
// copy-on-write fashion onto the provided exprFieldTypes.  This
// allows field-type info from deeper levels in the expr tree to
// shadow or override field-type info from parent levels.
func ProcessConjunctFieldTypes(
	indexedFields FieldInfos, identifiers Identifiers,
	exprs expression.Expressions, exprFieldTypes FieldTypes) (
	exprsOut expression.Expressions, exprFieldTypesOut FieldTypes,
	needsFiltering bool, ok bool) {
	p := &CheckFieldTypeInfo{
		IndexedFields:  indexedFields,
		Identifiers:    identifiers,
		Exprs:          exprs,
		ExprFieldTypes: exprFieldTypes,
	}

OUTER:
	for _, expr := range exprs {
		for _, f := range RegisteredCheckFieldTypeFuncs {
			r := f.Func(p, expr)
			if r == NotSargable {
				return nil, nil, false, false
			}

			if r == Match {
				continue OUTER
			}

			// r == NotMatch, so continue with next check func.
		}

		// None of the checks matched, so keep the expr.
		p.ExprsOut = append(p.ExprsOut, expr)
	}

	if len(p.Learned) <= 0 {
		return p.ExprsOut, p.ExprFieldTypes, false, true
	}

	return p.ExprsOut, append(FieldTypes{p.Learned}, p.ExprFieldTypes...),
		false, true
}

// ------------------------------------------------

type CheckFieldTypeInfo struct {
	// The following are immutable.
	IndexedFields  FieldInfos
	Identifiers    Identifiers
	Exprs          expression.Expressions
	ExprFieldTypes FieldTypes

	// The following are mutable.
	Learned  map[FieldTrack]string
	ExprsOut expression.Expressions
}

func (p *CheckFieldTypeInfo) AddLearning(
	fieldPath []string, fieldSuffix []string, fieldType string) bool {
	fieldTrack := FieldTrack(strings.Join(fieldPath, "."))
	if len(fieldSuffix) > 0 {
		fieldTrack = fieldTrack + "." + FieldTrack(strings.Join(fieldSuffix, "."))
	}

	t, ok := p.ExprFieldTypes.Lookup(fieldTrack)
	if !ok && p.Learned != nil {
		t, ok = p.Learned[fieldTrack]
	}
	if ok && t != fieldType {
		return false // Different type than previous learnings.
	}

	if !ok { // First time we learned type info for this field.
		if p.Learned == nil {
			p.Learned = map[FieldTrack]string{}
		}
		p.Learned[fieldTrack] = fieldType
	}

	return true
}

// ------------------------------------------------

type CheckFieldTypeResult int

const (
	NotSargable CheckFieldTypeResult = iota
	NotMatch
	Match
)

type CheckFieldTypeFunc func(*CheckFieldTypeInfo, expression.Expression) CheckFieldTypeResult

type RegisteredCheckFieldTypeFunc struct {
	Name string
	Func CheckFieldTypeFunc
}

var RegisteredCheckFieldTypeFuncs = []RegisteredCheckFieldTypeFunc{
	{"CheckFieldTypeNumber", CheckFieldTypeNumber},
	{"CheckFieldTypeString", CheckFieldTypeString},
}

// ------------------------------------------------

// CheckFieldTypeString() implements the CheckFieldTypeFunc() signature,
// and looks for range comparisons that tells us that a field is a string.
// For example... ("" <= `t`.`a`) AND (`t`.`a` < []),
// which is how ISSTRING(t.a) is simplified by N1QL.
func CheckFieldTypeString(p *CheckFieldTypeInfo,
	expr expression.Expression) CheckFieldTypeResult {
	return p.CheckFieldTypeByLoHiBounds(expr, "string",
		"le", value.EMPTY_STRING_VALUE,
		"lt", value.EMPTY_ARRAY_VALUE)
}

// CheckFieldTypeNumber() implements the CheckFieldTypeFunc() signature,
// and looks for range comparisons that tells us that a field is a number.
// For example... (true < `t`.`a`) AND (`t`.`a` < ""),
// which is how ISNUMBER(t.a) is simplified by N1QL.
func CheckFieldTypeNumber(p *CheckFieldTypeInfo,
	expr expression.Expression) CheckFieldTypeResult {
	return p.CheckFieldTypeByLoHiBounds(expr, "number",
		"lt", value.TRUE_VALUE,
		"lt", value.EMPTY_STRING_VALUE)
}

// ------------------------------------------------

func (p *CheckFieldTypeInfo) CheckFieldTypeByLoHiBounds(
	expr expression.Expression, fieldType string,
	loComp string, loValue value.Value,
	hiComp string, hiValue value.Value) CheckFieldTypeResult {
	if len(p.ExprsOut) < 1 {
		return NotMatch
	}

	// Check (loValue <= `t`.`a`).
	exprLo := p.ExprsOut[len(p.ExprsOut)-1]

	bfLo, ok := exprLo.(expression.BinaryFunction)
	if !ok || bfLo.Name() != loComp {
		return NotMatch
	}

	cLo, ok := bfLo.First().(*expression.Constant)
	if !ok || !cLo.Value().Equals(loValue).Truth() {
		return NotMatch
	}

	fiLo, suffixLo := p.IndexedFields.Find(p.Identifiers, bfLo.Second(), nil)
	if fiLo == nil {
		return NotMatch
	}

	if fiLo.FieldType != "" && fiLo.FieldType != fieldType {
		return NotSargable
	}

	// Check (`t`.`a` < hiValue).
	bfHi, ok := expr.(expression.BinaryFunction)
	if !ok || bfHi.Name() != hiComp {
		return NotMatch
	}

	fiHi, suffixHi := p.IndexedFields.Find(p.Identifiers, bfHi.First(), nil)
	if fiHi == nil {
		return NotMatch
	}

	if fiHi != fiLo || len(suffixHi) != len(suffixLo) {
		return NotMatch
	}

	for si, s := range suffixHi {
		if s != suffixLo[si] {
			return NotMatch
		}
	}

	cHi, ok := bfHi.Second().(*expression.Constant)
	if !ok || !cHi.Value().Equals(hiValue).Truth() {
		return NotMatch
	}

	// Add to learnings if no conflicts.
	if !p.AddLearning(fiHi.FieldPath, suffixHi, fieldType) {
		return NotSargable
	}

	// Filter the exprLo & exprHi since their type info was learned.
	p.ExprsOut = p.ExprsOut[0 : len(p.ExprsOut)-1]

	return Match
}
