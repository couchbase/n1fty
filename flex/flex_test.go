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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/planner"
)

func parseStatement(t *testing.T, stmt string) *algebra.Subselect {
	s, err := n1ql.ParseStatement(stmt, "" /* namespace */)
	if err != nil {
		t.Errorf("got err: %v", err)
	}
	return s.(*algebra.Select).Subresult().(*algebra.Subselect)
}

// ------------------------------------------------------------------

func TestParseStatement(t *testing.T) {
	s := parseStatement(t, "SELECT * FROM b as bb WHERE bb.x > 10")

	fmt.Printf("==========\n")
	fmt.Printf("s: %v\n", s)
	fmt.Printf(" From: %v\n", s.From())
	fmt.Printf("  PrimaryTerm: %#v\n", s.From().PrimaryTerm())
	fmt.Printf("   Alias: %v\n", s.From().PrimaryTerm().Alias())
	fmt.Printf(" Where: %v\n", s.Where())
	fmt.Printf("  Field: %#v\n", s.Where().Children()[1])
	fmt.Printf("    0 - Identifier: %#v\n", s.Where().Children()[1].Children()[0]) // identifier: "b"
	fmt.Printf("    1 - FieldName: %#v\n", s.Where().Children()[1].Children()[1])  // name: "x"

	// ------------------------------------------------------------------

	s = parseStatement(t, "SELECT * FROM b JOIN c ON b.f = c.f"+
		" WHERE b.x > 10")

	fmt.Printf("==========\n")
	fmt.Printf("s: %v\n", s)
	fmt.Printf(" From: %v\n", s.From())
	fmt.Printf("  PrimaryTerm: %#v\n", s.From().PrimaryTerm())
	fmt.Printf("   Alias: %v\n", s.From().PrimaryTerm().Alias())
	fmt.Printf(" Where: %v\n", s.Where())
	fmt.Printf("  Field: %#v\n", s.Where().Children()[1])
	fmt.Printf("    0 - Identifier: %#v\n", s.Where().Children()[1].Children()[0]) // identifier: "b"
	fmt.Printf("    1 - FieldName: %#v\n", s.Where().Children()[1].Children()[1])  // name: "x"

	// ------------------------------------------------------------------

	s = parseStatement(t, "SELECT * FROM b JOIN c ON b.f = c.f"+
		" LET z = c.x"+
		" WHERE b.x > 10 AND b.x.y < 100 AND ISSTRING(c.z)")

	fmt.Printf("==========\n")
	fmt.Printf("s: %v\n", s.Where()) // Not flattened.

	expr := s.Where()
	expr, _ = planner.NewDNF(expr, false, true).Map(expr)

	// issue: the LET bindings are not incorporated into the WHERE.

	fmt.Printf(" dnf: %v\n", expr) // Flattened, but DNF'ed.
	fmt.Printf(" let[0]: %#v\n", s.Let()[0])

	// issue: can have flattening of AND's, or DNF, but not just 1.
}

// ------------------------------------------------------------------

func TestFlexSargable(t *testing.T) {
	var indexedFieldsZ FieldInfos // For testing nil.

	indexedFields0 := FieldInfos{}
	indexedFieldsA := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
	}
	indexedFieldsAB := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
		&FieldInfo{FieldPath: []string{"b"}},
	}

	tests := []struct {
		about string

		from  []string
		let   string
		where string

		indexedFields  FieldInfos
		supportedExprs []SupportedExpr

		expectFieldTracks FieldTracks
		expectExact       bool
		expectFlexBuild   *FlexBuild
		expectErr         error
	}{
		{where: "true"},

		{where: "true",
			indexedFields: indexedFieldsZ},
		{where: "true",
			indexedFields: indexedFields0},
		{where: "true",
			indexedFields: indexedFieldsA},
		{where: "true",
			indexedFields: indexedFieldsAB},

		{where: `a = "hi"`,
			indexedFields: indexedFields0},

		{where: `a = "hi"`,
			indexedFields: indexedFieldsA,
			expectExact:   true, // Not sargable, so needsFiltering is false.
		},

		{where: `b = "hi"`,
			indexedFields: indexedFieldsA},

		{where: `a = "hi"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectExact:       true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hi"`},
			},
		},

		{about: `the reverse of a = "hi" is also sargable`,
			where:         `"hi" = a`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectExact:       true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hi"`},
			},
		},

		{where: `x = "hi"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
		},

		{where: "a = \"hello\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
			},
		},

		{where: `x = "hi" AND a = "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
				},
			},
		},

		{where: `a = "hello" AND b = 123`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "number", `123`},
					},
				},
			},
		},

		{where: `a = "hello" AND x = {} AND b = 123`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
				FieldTrack("b"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "number", `123`},
					},
				},
			},
		},

		{about: `the type of b in the index is number -- so, not sargable`,
			where:         `b = "string-not-a-number"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectExact: true,
		},

		{about: `not sargable due to non-constant value`,
			where:         `a = UPPER("hi")`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `not sargable due to advanced reference to indexed field a`,
			where:         `UPPER(a)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `not sargable due to advanced reference to indexed field b`,
			where:         `a = "hi" AND UPPER(b)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{where: `a = "hello" AND (b = 123 OR b = 222)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
				FieldTrack("b"): 2, // Because b is used twice.
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "b", "number", `123`},
							},
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "b", "number", `222`},
							},
						},
					},
				},
			},
		},

		{about: `not-sargable due to field ccc in the OR`,
			where:         `a = "hello" AND (b = 123 OR b = 222 OR ccc = 333)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectExact: true,
		},

		{where: `a = "hello" AND x = 999 AND (b = 123 OR b = 222)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
				FieldTrack("b"): 2, // Because b is used twice.
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "b", "number", `123`},
							},
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "b", "number", `222`},
							},
						},
					},
				},
			},
		},

		{about: `sargable but needs false-positive filtering for field x`,
			where:         `a = "hello" AND (b = 123 OR (b = 222 AND a = "y" AND x = 9))`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 2, // Because a is used twice.
				FieldTrack("b"): 2, // Because b is used twice.
			},
			expectExact: false, // Because of x.
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "b", "number", `123`},
							},
							{
								Kind: "conjunct",
								Children: []*FlexBuild{
									{
										Kind: "expr",
										Data: []string{"eqFieldConstant", "b", "number", `222`},
									},
									{
										Kind: "expr",
										Data: []string{"eqFieldConstant", "a", "string", `"y"`},
									},
								},
							},
						},
					},
				},
			},
		},

		{about: `test for top-level dynamic indexing`,
			where:         `a = "hello" AND c = "yay"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
				FieldTrack("c"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "c", "string", `"yay"`},
					},
				},
			},
		},

		{about: `test for top-level dynamic indexing with addr.city`,
			where:         `a = "hello" AND addr.city = "yay"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"):         1,
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"yay"`},
					},
				},
			},
		},

		{about: `test for dynamic indexing with non-"" prefix`,
			where:         `a = "hello" AND c = "yay"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
		},

		{about: `test for addr dynamic indexing with addr.state/city`,
			where:         "addr.state = \"ny\" AND addr.city = \"nyc\"",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.state"): 1,
				FieldTrack("addr.city"):  1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.state", "string", `"ny"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `test for addr dynamic indexing with addr.state and 2 cities`,
			where:         "addr.state = \"ny\" AND (addr.city = \"nyc\" OR addr.city = \"buffalo\")",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.state"): 1,
				FieldTrack("addr.city"):  2, // Because of nyc and buffalo.
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.state", "string", `"ny"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
							},
							{
								Kind: "expr",
								Data: []string{"eqFieldConstant", "addr.city", "string", `"buffalo"`},
							},
						},
					},
				},
			},
		},

		{about: `test non-exact addr dynamic indexing with addr.city`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 1,
			},
			expectExact: false, // Because of the "a" field.
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `test explicit and dynamic indexing of addr`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"):         1,
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `test explicit and dynamic non-top-level indexing`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"):         1,
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `test nested indexing`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"):         1,
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `test nested indexing`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr"}, // Doesn't cover addr.city.
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with the mismatched type`,
			where:         "a = \"hello\" AND addr.city = 123",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		// ----------------------------------------------------------

		{about: `test map/dict syntax with constant string key`,
			where:         "a[\"city\"] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "city"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `test map/dict syntax with field as key`,
			where:         "a[b] = \"nyc\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "city"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `test map/dict syntax with field key b on field path`,
			where:         "a[b] = \"nyc\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `test nested indexing with map/dict syntax and conjunct`,
			where:         "a = \"hello\" AND addr[\"city\"] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
				},
			},
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "cityfieldName"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "xyz[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "xyz[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "cityfieldName"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `not sargable - test map/dict syntax on dynamic field`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `not sargable - test map/dict syntax on dynamic field`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a", "cityFieldName"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `not sargable - test map/dict syntax on dynamic field w/ function expr`,
			where:         "a[UPPER(cityFieldName)] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a", "cityFieldName"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `not sargable - test map/dict syntax on dynamic field`,
			where:         "xyzw[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `test nested indexing with map/dict syntax on dynamic field`,
			where:         "addr[\"city\"] = \"nyc\"",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `test nested indexing with map/dict syntax on top-level dynamic field`,
			where:         "addr[\"city\"] = \"nyc\"",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `test conjunct map/dict syntax on dynamic field`,
			where:         "a = \"hello\" AND addr[\"city\"] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with function on nested value`,
			where:         "a = \"hello\" AND ROUND(b.geopoint.lat = \"hi\")",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `test dynamic indexing with function on nested array value`,
			where:         "a = \"hello\" AND ROUND(b.geopoint[1] = \"hi\")",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `test dynamic indexing with nested functional array value`,
			where:         "a = \"hello\" AND b.geopoint[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `test dynamic indexing with direct nested functional array value`,
			where:         "a = \"hello\" AND b[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `test dynamic indexing with array value`,
			where:         "a = \"hello\" AND b.pets[0] = \"fluffy\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectExact: true,
		},

		{about: `test non-dynamic, nested value`,
			where:         "a = \"hello\" AND b.x.y = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `test non-dynamic indexing of nested functional array value`,
			where:         "a = \"hello\" AND b[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		// --------------------------------------------------------

		{about: `prepared statement, named parameter not-sargable, not enough type info`,
			where:         "a = $paramX",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `prepared statement, named parameter`,
			where:         "ISSTRING(a) AND a = $paramX",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `$paramX`},
					},
				},
			},
		},

		{about: `prepared statement, positional parameter not-sargable, not enough type info`,
			where:         "a = $1",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `prepared statement, positional parameter`,
			where:         "ISSTRING(a) AND a = $1",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `$1`},
					},
				},
			},
		},

		// --------------------------------------------------------

		{about: `ANY-IN-SATISFIES syntax basic test`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
			},
		},

		{about: `ANY-AND-EVERY-IN-SATISFIES syntax basic test`,
			where:         "ANY AND EVERY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: false, // Due to the AND EVERY.
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
			},
		},

		{about: `ANY-IN-SATISFIES not-sargable with complex IN expression`,
			where:         "ANY v IN UPPER(a.b) SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `ANY-IN-SATISFIES not-sargable SATISFIES expr`,
			where:         "ANY v IN a SATISFIES true END",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `ANY-IN-SATISFIES not-sargable SATISFIES expr`,
			where:         "ANY v IN a.b SATISFIES true END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `ANY-IN-SATISFIES syntax, not exact`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END AND x = 123",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
				},
			},
		},

		{about: `not-sargable ANY-IN-SATISFIES`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `top-level dynamic ANY-IN-SATISFIES`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
			},
		},

		{about: `top-level dynamic ANY-AND-EVERY-IN-SATISFIES`,
			where:         "ANY AND EVERY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
			},
		},

		{about: `child dynamic ANY-IN-SATISFIES`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
			},
		},

		{about: `ANY-IN-SATISFIES syntax, composite condition`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" AND v.city = \"sf\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 2,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"sf"`},
					},
				},
			},
		},

		{about: `ANY-IN-SATISFIES syntax, multiple fields`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END AND b = \"sf\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
				FieldTrack("b"):        1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "string", `"sf"`},
					},
				},
			},
		},

		{about: `ANY-IN-SATISFIES syntax, multiple fields`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END OR b = \"sf\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
				FieldTrack("b"):        1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "disjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "string", `"sf"`},
					},
				},
			},
		},

		{about: `ANY-IN-SATISFIES syntax, multiple fields`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" AND b = \"sf\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
				FieldTrack("b"):        1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "string", `"sf"`},
					},
				},
			},
		},

		{about: `ANY-AND-EVERY-IN-SATISFIES syntax, multiple fields`,
			where:         "ANY AND EVERY v IN a.b SATISFIES v.city = \"nyc\" AND b = \"sf\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
				FieldTrack("b"):        1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a.b.city", "string", `"nyc"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "string", `"sf"`},
					},
				},
			},
		},

		{about: `ANY-IN-SATISFIES not-sargable with multiple bindings`,
			where:         "ANY v IN a.b, vv IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `ANY-IN-SATISFIES not-sargable with multiple bindings`,
			where:         "ANY v IN a.b, vv IN a.b SATISFIES vv.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `ANY-IN-SATISFIES not-sargable with multiple, chained bindings`,
			where:         "ANY v IN a, vv IN v SATISFIES vv.b.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "string",
				},
			},
			expectExact: true,
		},

		{about: `ANY-IN-SATISFIES in another satisfies`,
			where:         "ANY v IN a.b SATISFIES (ANY w IN v.c.d SATISFIES w.city = \"nyc\" END) END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "c", "d", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.c.d.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.c.d.city", "string", `"nyc"`},
			},
		},

		{about: `ANY-AND-EVERY-IN-SATISFIES in another satisfies`,
			where:         "ANY v IN a.b SATISFIES (ANY AND EVERY w IN v.c.d SATISFIES w.city = \"nyc\" END) END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "c", "d", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.c.d.city"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b.c.d.city", "string", `"nyc"`},
			},
		},

		// ------------------------------------------------------------------

		{about: `test LET`,
			let:           `c = a`,
			where:         `c = "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
			},
		},

		{about: `test LET conjunct`,
			let:           `c = addr.city`,
			where:         `a = "hello" AND c = "yay"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"):         1,
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"yay"`},
					},
				},
			},
		},

		{about: `test LET disjunct, double-c`,
			let:           `c = addr.city`,
			where:         `c = "hello" OR c = "yay"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 2,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "disjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "addr.city", "string", `"yay"`},
					},
				},
			},
		},

		{about: `test LET not-sargable on complex expression`,
			let:           `c = UPPER(a)`,
			where:         `c = "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectExact: false,
		},

		{about: `chained LET variables`,
			let:           `b = a, c = b, d = c, e = d`,
			where:         `d = "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c.b, e = d`,
			where:         `e = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b", "string", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c, e = d.b`,
			where:         `e = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b", "string", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c, e = d`,
			where:         `e.b = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.b", "string", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c.x.y.z, e = d`,
			where:         `e = "hello"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "x", "y", "z"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.x.y.z"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a.x.y.z", "string", `"hello"`},
			},
		},

		// ------------------------------------------------------------------

		{about: `test UNNEST`,
			from:          []string{"bucket", "UNNEST addr AS a"},
			where:         `a.city = "nyc"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
			},
		},

		{about: `test UNNEST with top-level dynamic indexing`,
			from:          []string{"bucket", "UNNEST addr AS a"},
			where:         `a.city = "nyc"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "string",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "addr.city", "string", `"nyc"`},
			},
		},

		{about: `test chained UNNEST's`,
			from:          []string{"bucket", "UNNEST addr AS a", "UNNEST `a`.phones AS p"},
			where:         `p.areaCode = 650`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "phones", "areaCode"},
					ValueType: "number",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.phones.areaCode"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "addr.phones.areaCode", "number", `650`},
			},
		},

		{about: `test chained UNNEST's with dynamic indexing`,
			from:          []string{"bucket", "UNNEST addr AS a", "UNNEST `a`.phones AS p"},
			where:         `p.areaCode = 650`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "number",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.phones.areaCode"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "addr.phones.areaCode", "number", `650`},
			},
		},

		{about: `test non-chained UNNEST's`,
			from:          []string{"bucket", "UNNEST address AS a", "UNNEST `bucket`.phones AS p"},
			where:         `a.city = "sf" AND p.provider = "verizon"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"address", "city"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"phones", "provider"},
					ValueType: "string",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("address.city"):    1,
				FieldTrack("phones.provider"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "address.city", "string", `"sf"`},
					},
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "phones.provider", "string", `"verizon"`},
					},
				},
			},
		},

		// ------------------------------------------------------------------

		{about: "test number inequality - not-sargable due to FieldTypeCheck",
			where:         `b < 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectExact: true,
		},

		{about: "test number inequality lt",
			where:         `ISNUMBER(b) AND b < 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"ltFieldConstant", "b", "number", `100`},
					},
				},
			},
		},

		{about: "test number inequality le",
			where:         `ISNUMBER(b) AND b <= 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"leFieldConstant", "b", "number", `100`},
					},
				},
			},
		},

		{about: "test number inequality gt",
			where:         `ISNUMBER(b) AND b > 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"gtFieldConstant", "b", "number", `100`},
					},
				},
			},
		},

		{about: "test number inequality ge",
			where:         `ISNUMBER(b) AND b >= 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"geFieldConstant", "b", "number", `100`},
					},
				},
			},
		},

		{about: "test number inequality eq",
			where:         `ISNUMBER(b) AND b = 100`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "number", `100`},
					},
				},
			},
		},

		// ------------------------------------------------------------------

		{about: "test string inequality - not-sargable due to FieldTypeCheck",
			where:         `b < "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectExact: true,
		},

		{about: "test string inequality lt",
			where:         `ISSTRING(b) AND b < "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"ltFieldConstant", "b", "string", `"hello"`},
					},
				},
			},
		},

		{about: "test string inequality le",
			where:         `ISSTRING(b) AND b <= "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"leFieldConstant", "b", "string", `"hello"`},
					},
				},
			},
		},

		{about: "test string inequality gt",
			where:         `ISSTRING(b) AND b > "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"gtFieldConstant", "b", "string", `"hello"`},
					},
				},
			},
		},

		{about: "test string inequality ge",
			where:         `ISSTRING(b) AND b >= "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"geFieldConstant", "b", "string", `"hello"`},
					},
				},
			},
		},

		{about: "test string inequality eq",
			where:         `ISSTRING(b) AND b = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"eqFieldConstant", "b", "string", `"hello"`},
					},
				},
			},
		},

		// ------------------------------------------------------------------

		{about: "test LIKE",
			// `a LIKE "hello%"` is rewritten as...
			//   (("hello" <= (`bucket`.`a`)) and ((`bucket`.`a`) < "hellp"))
			// and, note the "hello" versus "hellp".
			where:         `a LIKE "hello%"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"a"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 2,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"geFieldConstant", "a", "string", `"hello"`},
					},
					{
						Kind: "expr",
						Data: []string{"ltFieldConstant", "a", "string", `"hellp"`},
					},
				},
			},
		},

		{about: "test LIKE",
			// `a LIKE "hello"` is rewritten as ((`bucket`.`a`) = "hello")).
			where:         `a LIKE "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprNoop{},
				&SupportedExprCmpFieldConstant{
					Cmp:       "eq",
					FieldPath: []string{"a"},
					ValueType: "string",
				},
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"a"},
					ValueType:      "string",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "expr",
				Data: []string{"eqFieldConstant", "a", "string", `"hello"`},
			},
		},

		// ------------------------------------------------------------------

		{about: "test BETWEEN",
			// "x BETWEEN exprA AND exprB" is rewritten by DNF as...
			// "(AND (GE x exprA) (LE x exprB))".
			where:         `a BETWEEN 10 AND 100`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"a"},
					ValueType:      "number",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 2,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "expr",
						Data: []string{"geFieldConstant", "a", "number", `10`},
					},
					{
						Kind: "expr",
						Data: []string{"leFieldConstant", "a", "number", `100`},
					},
				},
			},
		},
	}

	for testi, test := range tests {
		fmt.Printf("-----------------\n")
		fmt.Printf("testi: %d %s\n", testi, test.about)

		from := test.from
		if len(from) <= 0 {
			from = []string{"bucket"}
		}
		from0 := from[0]

		fromClause := " FROM `" + from0 + "`"

		if len(from) > 1 {
			fromClause = fromClause + " " + strings.Join(from[1:], " ")
		}

		letClause := ""
		if test.let != "" {
			letClause = " LET " + test.let
		}

		stmt := "SELECT * " + fromClause + letClause + " WHERE " + test.where

		s := parseStatement(t, stmt)
		if s == nil {
			t.Errorf("expected s")
		}

		exprWhere := s.Where()

		exprWhereSimplified, _ := planner.NewDNF(exprWhere,
			true /* like */, false /* doDNF */).Map(exprWhere)

		identifiers := Identifiers{Identifier{Name: from0}}

		if len(test.from) > 1 {
			var err error
			identifiers, err = PushUnnests(identifiers, s.From())
			if err != nil {
				t.Fatalf("PushUnnests err: %v", err)
			}
		}

		if test.let != "" {
			var ok bool
			identifiers, ok = identifiers.PushBindings(s.Let(), -1)
			if !ok {
				t.Fatalf("identifiers.PushBindings not ok")
			}
		}

		fi := &FlexIndex{
			IndexedFields:  test.indexedFields,
			SupportedExprs: test.supportedExprs,
		}

		fieldTracks, needsFiltering, flexBuild, err := fi.Sargable(
			identifiers, exprWhereSimplified, nil)
		if err != test.expectErr {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %#v\n"+
				"  mismatch err: %v",
				testi, test, exprWhereSimplified, err)
		}

		if !reflect.DeepEqual(fieldTracks, test.expectFieldTracks) {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %#v\n"+
				"  mismatch expected with fieldTracks: %v",
				testi, test, exprWhereSimplified, fieldTracks)
		}

		if needsFiltering != !test.expectExact {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %#v\n"+
				"  mismatch expected with needsFiltering: %v",
				testi, test, exprWhereSimplified, needsFiltering)
		}

		if !reflect.DeepEqual(flexBuild, test.expectFlexBuild) {
			j, _ := json.Marshal(flexBuild)
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %#v\n"+
				"  mismatch expected with flexBuild: %#v\n  json: %s",
				testi, test, exprWhereSimplified, flexBuild, j)
		}
	}
}
