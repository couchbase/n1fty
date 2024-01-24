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
)

type Identifier struct {
	Name      string
	Expansion []string // May be empty when no expansion.
}

type Identifiers []Identifier // A left-pushed stack of identifiers.

// ---------------------------------------------------------------

func (a Identifiers) Push(bs expression.Bindings, max int) (Identifiers, bool) {
	rootIdentifier := a[len(a)-1].Name

	for i, b := range bs {
		if max > 0 && max <= i {
			return nil, false
		}

		if s, ok := ExpressionFieldPathSuffix(a, b.Expression(), nil, nil); ok {
			a = append(Identifiers{Identifier{
				Name:      b.Variable(),
				Expansion: append([]string{rootIdentifier}, s...),
			}}, a...) // Allocs new stack for left-push / copy-on-write.
		}
	}

	return a, true
}

// ReverseExpand finds the identifierName in the identifiers and on
// success will reverse-append the expansions onto the out slice.
// For example, if identifiers is...
//
//	[ Identifier{"w",   ["v", "address", "city"]},
//	  Identifier{"v",   ["emp", "locations"]},
//	  Identifier{"emp", nil} ],
//
// and identifierName is "w", and out is [], then returned will be...
//
//	["city", "address", "locations"], true
func (a Identifiers) ReverseExpand(
	identifierName string, out []string) ([]string, bool) {
	for _, identifier := range a {
		if identifier.Name == identifierName {
			if len(identifier.Expansion) <= 0 {
				return out, true
			}

			for i := len(identifier.Expansion) - 1; i > 0; i-- {
				out = append(out, identifier.Expansion[i])
			}

			identifierName = identifier.Expansion[0]
		}
	}

	return out[:0], false
}

// ---------------------------------------------------------------

// A field name is part of a field path, like "name", or "city".
//
// A field path is an absolute, fully qualified []string, like...
//   []string{"offices", "addr", "city"}.
//
// A field track is a field path joined by "." separator, like...
//   "offices.addr.city".

type FieldTrack string
type FieldTracks map[FieldTrack]int

// ---------------------------------------------------------------

type FieldInfo struct {
	FieldPath []string // Ex: ["name"], ["addr", "city"], [].
	FieldType string   // Ex: "text", "number", "boolean", "datetime".
	FieldDims int      // Applicable to FieldType "vector".
}

type FieldInfos []*FieldInfo

// ---------------------------------------------------------------

func (fieldInfos FieldInfos) Contains(fieldName, fieldType, analyzer string, dims int) bool {
	for _, fieldInfo := range fieldInfos {
		if fieldName == strings.Join(fieldInfo.FieldPath, ".") {
			if fieldType == fieldInfo.FieldType {
				if fieldType == "vector" {
					if dims == fieldInfo.FieldDims {
						return true
					}
				} else if len(analyzer) == 0 || analyzer == "keyword" {
					return true
				}
			}
		}
	}

	return false
}

// Given an expression.Field, finds the first FieldInfo that has a
// matching field-path prefix.  The suffixOut enables slice reuse.
func (fieldInfos FieldInfos) Find(a Identifiers, f expression.Expression,
	suffixOut []string) (*FieldInfo, []string) {
	var ok bool

	for _, fieldInfo := range fieldInfos {
		suffixOut, ok = ExpressionFieldPathSuffix(a, f,
			fieldInfo.FieldPath, suffixOut[:0])
		if ok {
			return fieldInfo, suffixOut
		}
	}

	return nil, suffixOut[:0]
}

// ---------------------------------------------------------------

// Recursively checks if expr uses any of field in the fieldInfos,
// where the fields all resolve to the same root identifier scope.
//
// Example: if fieldInfos has 'price' and 'cost', then...
// - `rootIdentifier`.`price` > 100 ==> true.
// - `unknownIdentifier`.`cost` > 100 ==> false.

func CheckFieldsUsed(fieldInfos FieldInfos, a Identifiers,
	expr expression.Expression) (found bool, err error) {
	var fieldInfo *FieldInfo
	var suffix []string

	m := &expression.MapperBase{}

	m.SetMapper(m)

	m.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if fieldInfo == nil {
			if f, ok := e.(*expression.Field); ok {
				fieldInfo, suffix = fieldInfos.Find(a, f, suffix[:0])
			}

			return e, e.MapChildren(m)
		}

		return e, nil // When found, avoid additional recursion.
	})

	_, err = m.Map(expr)

	return fieldInfo != nil, err
}

// ---------------------------------------------------------------

// Checks whether an expr, such as
// `emp`.`locations`.`work`.`address`.`city`, is a nested fields
// reference against the identifiers (e.g., "emp"), and also has a
// given prefix (e.g., ["locations", "work"]), and if so returns the
// remaining field path suffix (e.g., ["address", "city"]).
// The prefix may be [].  The returned suffix may be [].
// The suffixOut allows for slice reuse.
func ExpressionFieldPathSuffix(a Identifiers, expr expression.Expression,
	prefix []string, suffixOut []string) ([]string, bool) {
	suffixRev := suffixOut[:0] // Reuse slice for reverse suffix.

	var visit func(e expression.Expression) bool // Declare for recursion.

	visit = func(e expression.Expression) bool {
		if f, ok := e.(*expression.Field); ok {
			suffixRev = append(suffixRev, f.Second().Alias())
			return visit(f.First())
		}

		if i, ok := e.(*expression.Identifier); ok {
			suffixRev, ok = a.ReverseExpand(i.Identifier(), suffixRev)
			return ok
		}

		return false
	}

	if !visit(expr) {
		return suffixOut[:0], false
	}

	// Compare suffixReverse (["city", "address", "work", "locations"])
	// with the prefix (["locations", "work"]).
	if len(prefix) > len(suffixRev) {
		return suffixOut[:0], false
	}

	for _, s := range prefix {
		if s != suffixRev[len(suffixRev)-1] {
			return suffixOut[:0], false
		}

		suffixRev = suffixRev[0 : len(suffixRev)-1]
	}

	if len(suffixRev) <= 0 {
		return suffixOut[:0], true
	}

	return reverseInPlace(suffixRev), true
}

func reverseInPlace(a []string) []string {
	last := len(a) - 1
	mid := len(a) / 2

	for i := 0; i < mid; i++ {
		a[i], a[last-i] = a[last-i], a[i]
	}

	return a
}
