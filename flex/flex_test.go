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
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/plannerbase"
)

func parseStatement(t *testing.T, stmt string) *algebra.Subselect {
	s, err := n1ql.ParseStatement(stmt)
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
	expr, _ = plannerbase.NewDNF(expr, false, true).Map(expr)

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
			expectExact:   false, // Not sargable, needsFiltering is true.
		},

		{where: `b = "hi"`,
			indexedFields: indexedFieldsA},

		{where: `a = "hi"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectExact:       true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hi"`},
			},
		},

		{about: `the reverse of a = "hi" is also sargable`,
			where:         `"hi" = a`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectExact:       true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hi"`},
			},
		},

		{where: `x = "hi"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
		},

		{where: "a = \"hello\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hello"`},
			},
		},

		{where: `x = "hi" AND a = "hello"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{FieldTrack("a"): 1},
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{where: `a = "hello" AND b = 123`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "number", `123`},
					},
				},
			},
		},

		{where: `a = "hello" AND x = {} AND b = 123`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "number", `123`},
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
			expectExact: false,
		},

		{about: `not sargable due to non-constant value`,
			where:         `a = UPPER("hi")`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `partially sargable due to advanced reference to indexed field b`,
			where:         `a = "hi" AND UPPER(b)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hi"`},
					},
				},
			},
		},

		{where: `a = "hello" AND (b = 123 OR b = 222)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "b", "number", `123`},
							},
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "b", "number", `222`},
							},
						},
					},
				},
			},
		},

		{about: `partially sargable due to field ccc in the OR`,
			where:         `a = "hello" AND (b = 123 OR b = 222 OR ccc = 333)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{where: `a = "hello" AND x = 999 AND (b = 123 OR b = 222)`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "b", "number", `123`},
							},
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "b", "number", `222`},
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
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "b", "number", `123`},
							},
							{
								Kind: "conjunct",
								Children: []*FlexBuild{
									{
										Kind: "cmpFieldConstant",
										Data: []string{"eq", "b", "number", `222`},
									},
									{
										Kind: "cmpFieldConstant",
										Data: []string{"eq", "a", "text", `"y"`},
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
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "c", "text", `"yay"`},
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
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"yay"`},
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
					ValueType:        "text",
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
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.state", "text", `"ny"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"nyc"`},
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
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.state", "text", `"ny"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "addr.city", "text", `"nyc"`},
							},
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "addr.city", "text", `"buffalo"`},
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
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"nyc"`},
					},
				},
			},
		},

		{about: `test dynamic indexing using property container name`,
			where:         "addr = \"nyc\"",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
		},

		{about: `test explicit and dynamic indexing of addr`,
			where:         "a = \"hello\" AND addr.city = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"nyc"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"nyc"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"nyc"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr"}, // Doesn't cover addr.city.
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with the mismatched type (partially sargable)`,
			where:         "a = \"hello\" AND addr.city = 123",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		// ----------------------------------------------------------

		{about: `test map/dict syntax with constant string key`,
			where:         "a[\"city\"] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "city"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `test map/dict syntax with field as key`,
			where:         "a[b] = \"nyc\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "city"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `test map/dict syntax with field key b on field path`,
			where:         "a[b] = \"nyc\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `test nested indexing with map/dict syntax and conjunct`,
			where:         "a = \"hello\" AND addr[\"city\"] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
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
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "cityfieldName"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `not-sargable - test map/dict syntax`,
			where:         "xyz[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
					ValueType: "text",
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
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `not sargable - test map/dict syntax on dynamic field`,
			where:         "a[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a", "cityFieldName"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `not sargable - test map/dict syntax on dynamic field w/ function expr`,
			where:         "a[UPPER(cityFieldName)] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a", "cityFieldName"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectExact: false,
		},

		{about: `not sargable - test function on dynamic field w/ function expr`,
			where:         "UPPER(a.cityFieldName)",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a", "cityFieldName"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
		},

		{about: `not sargable - test function on dynamic field w/ function expr`,
			where:         "UPPER(addr)",
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
		},

		{about: `partially sargable - test function on dynamic field w/ function expr`,
			where:         "a.b = \"hi\" AND UPPER(a.b)",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b", "text", `"hi"`},
					},
				},
			},
		},

		{about: `not sargable - test map/dict syntax on dynamic field`,
			where:         "xyzw[cityFieldName] = \"nyc\"",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "text",
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
					ValueType:        "text",
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
					ValueType:        "text",
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"addr"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with function on nested value (partially sargable)`,
			where:         "a = \"hello\" AND ROUND(b.geopoint.lat = \"hi\")",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with function on nested array value (partially sargable)`,
			where:         "a = \"hello\" AND ROUND(b.geopoint[1] = \"hi\")",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with nested functional array value (partially sargable)`,
			where:         "a = \"hello\" AND b.geopoint[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with direct nested functional array value (partially sargable)`,
			where:         "a = \"hello\" AND b[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test dynamic indexing with array value (partial sargability)`,
			where:         "a = \"hello\" AND b.pets[0] = \"fluffy\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"b"},
					ValueType:        "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test non-dynamic, nested value (partial sargability)`,
			where:         "a = \"hello\" AND b.x.y = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		{about: `test non-dynamic indexing of nested functional array value (partial sargability)`,
			where:         "a = \"hello\" AND b[ROUND(1)] = \"hi\"",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
				},
			},
		},

		// --------------------------------------------------------

		{about: `prepared statement, named parameter not-sargable, not enough type info`,
			where:         "a = $paramX",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `prepared statement, named parameter`,
			where:         "ISSTRING(a) AND a = $paramX",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `$paramX`},
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
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `prepared statement, positional parameter`,
			where:         "ISSTRING(a) AND a = $1",
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `$1`},
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
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.city", "text", `"nyc"`},
			},
		},

		{about: `ANY-AND-EVERY-IN-SATISFIES syntax basic test`,
			where:         "ANY AND EVERY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: false, // Due to the AND EVERY.
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.city", "text", `"nyc"`},
			},
		},

		{about: `ANY-IN-SATISFIES not-sargable with complex IN expression`,
			where:         "ANY v IN UPPER(a.b) SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "text",
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
					ValueType: "text",
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
					ValueType: "text",
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
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
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
					ValueType: "text",
				},
			},
			expectExact: false,
		},

		{about: `top-level dynamic ANY-IN-SATISFIES`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.city", "text", `"nyc"`},
			},
		},

		{about: `top-level dynamic ANY-AND-EVERY-IN-SATISFIES`,
			where:         "ANY AND EVERY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.city", "text", `"nyc"`},
			},
		},

		{about: `child dynamic ANY-IN-SATISFIES`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{"a"},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.city", "text", `"nyc"`},
			},
		},

		{about: `ANY-IN-SATISFIES syntax, composite condition`,
			where:         "ANY v IN a.b SATISFIES v.city = \"nyc\" AND v.city = \"sf\" END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "city"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 2,
			},
			expectExact: false, // multiple predicates within ANY-SATISFIES: MB-51888
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"sf"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "text", `"sf"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "text", `"sf"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.city"): 1,
				FieldTrack("b"):        1,
			},
			expectExact: false, // multiple predicates within ANY-SATISFIES: MB-51888
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "text", `"sf"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"b"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a.b.city", "text", `"nyc"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "text", `"sf"`},
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
					ValueType: "text",
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
					ValueType: "text",
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
					ValueType: "text",
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
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.c.d.city"): 1,
			},
			expectExact: false, // multiple predicates within ANY-SATISFIES: MB-51888
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.c.d.city", "text", `"nyc"`},
			},
		},

		{about: `ANY-AND-EVERY-IN-SATISFIES in another satisfies`,
			where:         "ANY v IN a.b SATISFIES (ANY AND EVERY w IN v.c.d SATISFIES w.city = \"nyc\" END) END",
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b", "c", "d", "city"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b.c.d.city"): 1,
			},
			expectExact: false,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b.c.d.city", "text", `"nyc"`},
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
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hello"`},
			},
		},

		{about: `test LET conjunct`,
			let:           `c = addr.city`,
			where:         `a = "hello" AND c = "yay"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"addr", "city"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"yay"`},
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
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "addr.city", "text", `"yay"`},
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
					ValueType: "text",
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
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c.b, e = d`,
			where:         `e = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b", "text", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c, e = d.b`,
			where:         `e = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b", "text", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c, e = d`,
			where:         `e.b = "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "b"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.b", "text", `"hello"`},
			},
		},

		{about: `chained LET variables`,
			let:           `c = a, d = c.x.y.z, e = d`,
			where:         `e = "hello"`,
			indexedFields: indexedFields0,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"a", "x", "y", "z"},
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.x.y.z"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a.x.y.z", "text", `"hello"`},
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
					ValueType: "text",
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "addr.city", "text", `"nyc"`},
			},
		},

		{about: `test UNNEST with top-level dynamic indexing`,
			from:          []string{"bucket", "UNNEST addr AS a"},
			where:         `a.city = "nyc"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath:        []string{},
					ValueType:        "text",
					FieldPathPartial: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("addr.city"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "addr.city", "text", `"nyc"`},
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
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "addr.phones.areaCode", "number", `650`},
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
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "addr.phones.areaCode", "number", `650`},
			},
		},

		{about: `test non-chained UNNEST's`,
			from:          []string{"bucket", "UNNEST address AS a", "UNNEST `bucket`.phones AS p"},
			where:         `a.city = "sf" AND p.provider = "verizon"`,
			indexedFields: indexedFieldsA,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"address", "city"},
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					FieldPath: []string{"phones", "provider"},
					ValueType: "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "address.city", "text", `"sf"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "phones.provider", "text", `"verizon"`},
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
			expectExact: false,
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
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "b", "number", `100`},
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
						Kind: "cmpFieldConstant",
						Data: []string{"le", "b", "number", `100`},
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
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "b", "number", `100`},
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
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "b", "number", `100`},
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "number", `100`},
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
					ValueType:      "text",
					FieldTypeCheck: true,
				},
			},
			expectExact: false,
		},

		{about: "test string inequality lt",
			where:         `ISSTRING(b) AND b < "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"b"},
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "b", "text", `"hello"`},
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
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"le", "b", "text", `"hello"`},
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
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "b", "text", `"hello"`},
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
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "b", "text", `"hello"`},
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
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "text", `"hello"`},
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
					ValueType:      "text",
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
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "text", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "text", `"hellp"`},
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
					ValueType: "text",
				},
				&SupportedExprCmpFieldConstant{
					Cmp:            "eq lt le gt ge",
					FieldPath:      []string{"a"},
					ValueType:      "text",
					FieldTypeCheck: true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "text", `"hello"`},
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
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "number", `10`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"le", "a", "number", `100`},
					},
				},
			},
		},

		// ------------------------------------------------------------------

		{about: "test dynamic string inequality lt",
			where:         `ISSTRING(a.b) AND a.b < "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:              "lt le gt ge",
					FieldPath:        []string{"a"},
					ValueType:        "text",
					FieldPathPartial: true,
					FieldTypeCheck:   true,
				},
			},
			expectFieldTracks: FieldTracks{
				FieldTrack("a.b"): 1,
			},
			expectExact: true,
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a.b", "text", `"hello"`},
					},
				},
			},
		},

		{about: "test dynamic string inequality lt when type of a.c not learned",
			where:         `ISSTRING(a.b) AND a.c < "hello"`,
			indexedFields: indexedFieldsAB,
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp:              "lt le gt ge",
					FieldPath:        []string{"a"},
					ValueType:        "text",
					FieldPathPartial: true,
					FieldTypeCheck:   true,
				},
			},
			expectExact: false,
		},

		// ------------------------------------------------------------------

		{about: "test dynamic string with UNNEST",
			from:          []string{"o", "UNNEST o.orderlines as orderline"},
			where:         `orderline.productId = "sugar22" AND (o.custId = "ccc" OR o.custId = "abc")`,
			indexedFields: FieldInfos{&FieldInfo{FieldPath: nil, FieldType: "string"}},
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp: "eq", FieldPath: nil, ValueType: "text",
					FieldPathPartial: true, FieldTypeCheck: false,
				},
				&SupportedExprCmpFieldConstant{
					Cmp:              "lt gt le ge",
					FieldPath:        nil,
					ValueType:        "text",
					FieldTypeCheck:   true,
					FieldPathPartial: true,
				},
			},
			expectExact: true,
			expectFieldTracks: FieldTracks{
				FieldTrack("orderlines.productId"): 1,
				FieldTrack("custId"):               2,
			},
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "orderlines.productId", "text", `"sugar22"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "custId", "text", `"ccc"`},
							},
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "custId", "text", `"abc"`},
							},
						},
					},
				},
			},
		},

		{about: "test dynamic string with UNNEST with DNF child array assertions",
			from: []string{"o", "UNNEST o.orderlines as orderline"},
			where: `orderline.productId = "sugar22"
                    AND (o.custId = "ccc" OR o.custId = "abc")
                    AND [] <= o.orderlines
                    AND o.orderlines < {}`,
			indexedFields: FieldInfos{
				&FieldInfo{FieldPath: []string{"orderlines"}, FieldType: "string"},
				&FieldInfo{FieldPath: nil, FieldType: "string"},
			},
			supportedExprs: []SupportedExpr{
				&SupportedExprCmpFieldConstant{
					Cmp: "eq", FieldPath: nil, ValueType: "text",
					FieldPathPartial: true, FieldTypeCheck: false,
				},
			},
			expectExact: true,
			expectFieldTracks: FieldTracks{
				FieldTrack("orderlines.productId"): 1,
				FieldTrack("custId"):               2,
			},
			expectFlexBuild: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "orderlines.productId", "text", `"sugar22"`},
					},
					{
						Kind: "disjunct",
						Children: []*FlexBuild{
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "custId", "text", `"ccc"`},
							},
							{
								Kind: "cmpFieldConstant",
								Data: []string{"eq", "custId", "text", `"abc"`},
							},
						},
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

		exprWhereSimplified, _ := plannerbase.NewDNF(exprWhere,
			true /* like */, false /* doDNF */).Map(exprWhere)

		identifiers := Identifiers{Identifier{Name: from0}}

		unnestBindings := gatherUnnestBindings(s.From(), nil)

		var ok bool
		identifiers, ok = identifiers.Push(unnestBindings, -1)
		if !ok {
			t.Fatalf("push unnestBindings not ok")
		}

		if test.let != "" {
			identifiers, ok = identifiers.Push(s.Let(), -1)
			if !ok {
				t.Fatalf("identifiers.Push not ok")
			}
		}

		fi := &FlexIndex{
			IndexedFields:  test.indexedFields,
			SupportedExprs: test.supportedExprs,
		}

		fieldTracks, needsFiltering, flexBuild, err := fi.Sargable(
			identifiers, exprWhereSimplified, nil, nil)
		if err != test.expectErr {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %+v\n"+
				"  mismatch err: %v",
				testi, test, exprWhereSimplified, err)
		}

		if !reflect.DeepEqual(fieldTracks, test.expectFieldTracks) {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %+v\n"+
				"  mismatch expected with fieldTracks: %v",
				testi, test, exprWhereSimplified, fieldTracks)
		}

		if needsFiltering != !test.expectExact {
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %+v\n"+
				"  mismatch expected with needsFiltering: %v",
				testi, test, exprWhereSimplified, needsFiltering)
		}

		if !reflect.DeepEqual(flexBuild, test.expectFlexBuild) {
			j, _ := json.Marshal(flexBuild)
			t.Fatalf("testi: %d, test: %+v\n  exprWhereSimplified: %+v\n"+
				"  mismatch expected with flexBuild: %#v\n  json: %s",
				testi, test, exprWhereSimplified, flexBuild, j)
		}
	}
}

// Recursively gather UNNEST bindings.
func gatherUnnestBindings(f algebra.FromTerm, a expression.Bindings) expression.Bindings {
	if f == nil {
		return a
	}

	if j, ok := f.(algebra.JoinTerm); ok {
		a = gatherUnnestBindings(j.Left(), a) // Left-most first.
	}

	if u, ok := f.(*algebra.Unnest); ok && !u.Outer() {
		a = append(a, expression.NewSimpleBinding(u.Alias(), u.Expression()))
	}

	return a
}
