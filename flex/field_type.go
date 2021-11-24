//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package flex

import (
	"strings"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

// Used to track field-type learnings, FieldTypes is a copy-on-write,
// left-side-pushed stack of immutable dictionaries.
type FieldTypes []map[FieldTrack]string

func (f FieldTypes) Lookup(k FieldTrack) (string, bool) {
	for _, m := range f {
		if v, ok := m[k]; ok {
			return v, true
		}
	}

	return "", false
}

// ---------------------------------------------------------------

// LearnConjunctFieldTypes examines the conjunct exprs (e.g., children
// of an "AND" expression) to discover field-type information. Only
// the expressions related to the given identifiers and indexedFields
// are considered.  It returns a (potentially filtered/simplified)
// copy of the exprs, where the filtering/simplification happens as
// field-type's are learned.
//
// The learned field-type information is pushed in immutable,
// copy-on-write fashion onto the provided exprFTs.  This allows
// field-type info from deeper levels in the expr tree to shadow or
// override field-type info from parent levels.
func LearnConjunctFieldTypes(indexedFields FieldInfos, ids Identifiers,
	exprs expression.Expressions, exprFTs FieldTypes) (
	exprsOut expression.Expressions, exprFTsOut FieldTypes, ok bool) {
	p := &ConjunctFieldTypes{
		IndexedFields:  indexedFields,
		Identifiers:    ids,
		Exprs:          exprs,
		ExprFieldTypes: exprFTs,
	}

OUTER:
	for _, expr := range exprs {
		for _, f := range RegisteredCFTFuncs {
			r := f.Func(p, expr)
			if r == "not-sargable" {
				return nil, nil, false
			}

			if r == "match" {
				continue OUTER
			}

			// r == "not-match", so continue to next func.
		}

		// None of the checks matched, so keep the expr.
		p.ExprsOut = append(p.ExprsOut, expr)
	}

	if len(p.Learned) <= 0 {
		return p.ExprsOut, p.ExprFieldTypes, true
	}

	return p.ExprsOut, append(FieldTypes{p.Learned}, p.ExprFieldTypes...), true
}

// ------------------------------------------------

// ConjunctFieldTypes represents state as a conjunct expression is
// examined and processed for field-type information.
type ConjunctFieldTypes struct {
	// The following are immutable.
	IndexedFields  FieldInfos
	Identifiers    Identifiers
	Exprs          expression.Expressions
	ExprFieldTypes FieldTypes

	// The following are mutated during the learning.
	Learned  map[FieldTrack]string
	ExprsOut expression.Expressions
}

func (p *ConjunctFieldTypes) AddLearning(
	fieldPath []string, fieldSuffix []string, fieldType string) bool {
	fieldTrack := FieldTrack(strings.Join(fieldPath, "."))
	if len(fieldSuffix) > 0 {
		if len(fieldTrack) > 0 {
			fieldTrack = fieldTrack + "."
		}
		fieldTrack = fieldTrack + FieldTrack(strings.Join(fieldSuffix, "."))
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

type RegisteredCFTFunc struct {
	Name string
	Func func(*ConjunctFieldTypes, expression.Expression) string
}

var RegisteredCFTFuncs = []RegisteredCFTFunc{
	{"CFTString", CFTString}, {"CFTNumber", CFTNumber}, {"CFTArray", CFTArray},
}

// ------------------------------------------------

// CFTString() looks for range comparisons that tells us that a field
// is a string.  For example... ("" <= `t`.`a`) AND (`t`.`a` < []),
// which is how ISSTRING(t.a) is simplified by N1QL.
func CFTString(p *ConjunctFieldTypes, e expression.Expression) string {
	return p.CheckFieldTypeLoHi(e, "string",
		"le,lt", value.EMPTY_STRING_VALUE, "lt,le", value.EMPTY_ARRAY_VALUE)
}

// CFTNumber() looks for range comparisons that tells us that a field
// is a number.  For example... (true < `t`.`a`) AND (`t`.`a` < ""),
// which is how ISNUMBER(t.a) is simplified by N1QL.
func CFTNumber(p *ConjunctFieldTypes, e expression.Expression) string {
	return p.CheckFieldTypeLoHi(e, "number",
		"lt,le", value.TRUE_VALUE, "lt,le", value.EMPTY_STRING_VALUE)
}

// CFTArray() looks for range comparisons that tells us that a field
// is an array.  For example... ([] <= `t`.`a`) AND (`t`.`a` < {}),
// which is how ISARRAY(t.a) is simplified by N1QL and UNNEST DNF.
func CFTArray(p *ConjunctFieldTypes, e expression.Expression) string {
	return p.CheckFieldTypeLoHi(e, "string", // Meant for child paths.
		"le,lt", value.EMPTY_ARRAY_VALUE, "lt", EMPTY_OBJECT_VALUE)
}

var EMPTY_OBJECT_VALUE = value.NewValue(map[string]interface{}{})

// ------------------------------------------------

func (p *ConjunctFieldTypes) CheckFieldTypeLoHi(
	exprHi expression.Expression, fType string,
	loComp string, loValue value.Value,
	hiComp string, hiValue value.Value) string {
	if len(p.ExprsOut) < 1 {
		return "not-match"
	}

	exprLo := p.ExprsOut[len(p.ExprsOut)-1]

	// Check (loValue <= `t`.`a`).
	bfLo, ok := exprLo.(expression.BinaryFunction)
	if !ok || !strings.Contains(loComp, bfLo.Name()) {
		return "not-match"
	}

	if bfLo.First().Static() == nil {
		return "not-match"
	}

	vLo := bfLo.First().Value()

	fiLo, suffixLo := p.IndexedFields.Find(p.Identifiers, bfLo.Second(), nil)
	if fiLo == nil {
		return "not-match"
	}

	// Check (`t`.`a` < hiValue).
	bfHi, ok := exprHi.(expression.BinaryFunction)
	if !ok || !strings.Contains(hiComp, bfHi.Name()) {
		return "not-match"
	}

	if bfHi.Second().Static() == nil {
		return "not-match"
	}

	vHi := bfHi.Second().Value()

	fiHi, suffixHi := p.IndexedFields.Find(p.Identifiers, bfHi.First(), nil)
	if fiHi == nil {
		return "not-match"
	}

	if fiHi != fiLo || len(suffixHi) != len(suffixLo) {
		return "not-match"
	}

	for si, s := range suffixHi {
		if s != suffixLo[si] {
			return "not-match"
		}
	}

	if vLo.Equals(loValue).Truth() && strings.HasPrefix(loComp, bfLo.Name()) &&
		vHi.Equals(hiValue).Truth() && strings.HasPrefix(hiComp, bfHi.Name()) {
		// Add to learnings if no conflicts.
		if !p.AddLearning(fiHi.FieldPath, suffixHi, fType) {
			return "not-sargable"
		}

		// Filter exprLo/exprHi as ISSTRING/NUMBER() was exactly learned.
		p.ExprsOut = p.ExprsOut[0 : len(p.ExprsOut)-1]

		return "match" // Ex: learned ("" <= fieldLo) AND (fieldHi < []).
	}

	if vLo.Type().String() == fType &&
		vHi.Type().String() == fType && vLo.Collate(vHi) <= 0 &&
		p.AddLearning(fiHi.FieldPath, suffixHi, fType) {
		p.ExprsOut = append(p.ExprsOut, exprHi)

		return "match" // Ex: learned ("A" < fieldLo) AND (fieldHi < "H").
	}

	return "not-match"
}
