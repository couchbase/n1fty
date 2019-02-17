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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
)

func TestBleveToFlexIndex(t *testing.T) {
	tests := []struct {
		m               *mapping.IndexMappingImpl
		expectFlexIndex *FlexIndex
		expectErr       string
	}{
		{
			m:         &mapping.IndexMappingImpl{},
			expectErr: "because there isn't a default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"not-handled": {},
				},
			},
			expectErr: "because there isn't a default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"not-handled": {},
				},
				DefaultMapping: &mapping.DocumentMapping{},
			},
			expectErr: "because there isn't a only default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "NOT-text", Analyzer: "keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "NOT-keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: false, /* Index FALSE */
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "",
							Index: true,
						},
					},
				},
				DefaultAnalyzer: "NOT-keyword",
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", /* Analyzer: "", */
							Index: true,
						},
					},
				},
				DefaultAnalyzer: "keyword",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: true,
						},
						{
							Name: "f2", Type: "text", Analyzer: "keyword",
							Index: false,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"addr": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"addr", "f1"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"addr", "f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"addr", "f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"addr": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
						"pets": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"addr", "f1"},
						FieldType: "string",
					},
					&FieldInfo{
						FieldPath: []string{"pets", "f1"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"addr", "f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"addr", "f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"pets", "f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"pets", "f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: true,
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: true,
				},
				DefaultAnalyzer:       "keyword",
				DefaultDateTimeParser: "disabled",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: nil,
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:              "eq",
						FieldPath:        nil,
						ValueType:        "string",
						FieldPathPartial: true,
					},
					&SupportedExprCmpFieldConstant{
						Cmp:              "lt gt le ge",
						FieldPath:        nil,
						ValueType:        "string",
						FieldTypeCheck:   true,
						FieldPathPartial: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:         true,
					Dynamic:         true,
					DefaultAnalyzer: "keyword",
				},
				DefaultDateTimeParser: "disabled",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: nil,
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:              "eq",
						FieldPath:        nil,
						ValueType:        "string",
						FieldPathPartial: true,
					},
					&SupportedExprCmpFieldConstant{
						Cmp:              "lt gt le ge",
						FieldPath:        nil,
						ValueType:        "string",
						FieldTypeCheck:   true,
						FieldPathPartial: true,
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"addr": {
							Enabled: true,
							Dynamic: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
				DefaultAnalyzer:       "keyword",
				DefaultDateTimeParser: "disabled",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"addr", "f1"},
						FieldType: "string",
					},
					&FieldInfo{
						FieldPath: []string{"addr"},
						FieldType: "string",
					},
				},
				SupportedExprs: []SupportedExpr{
					&SupportedExprCmpFieldConstant{
						Cmp:       "eq",
						FieldPath: []string{"addr", "f1"},
						ValueType: "string",
					},
					&SupportedExprCmpFieldConstant{
						Cmp:            "lt gt le ge",
						FieldPath:      []string{"addr", "f1"},
						ValueType:      "string",
						FieldTypeCheck: true,
					},
					&SupportedExprCmpFieldConstant{
						Cmp:              "eq",
						FieldPath:        []string{"addr"},
						ValueType:        "string",
						FieldPathPartial: true,
					},
					&SupportedExprCmpFieldConstant{
						Cmp:              "lt gt le ge",
						FieldPath:        []string{"addr"},
						ValueType:        "string",
						FieldTypeCheck:   true,
						FieldPathPartial: true,
					},
				},
			},
		},
	}

	for _, test := range tests {
		flexIndex, err := BleveToFlexIndex(test.m)
		if (err != nil) != (test.expectErr != "") {
			t.Fatalf("test: %+v,\n err mismatch, got: %v",
				test, err)
		}

		if !reflect.DeepEqual(flexIndex, test.expectFlexIndex) {
			jm, _ := json.Marshal(test.m)
			jefi, _ := json.Marshal(test.expectFlexIndex)
			jfi, _ := json.Marshal(flexIndex)

			t.Errorf("test: %+v,\n jm: %s\n expectFlexIndex: %s\n flexIndex mismatch, got: %s",
				test, jm, jefi, jfi)
		}
	}
}

func TestFlexBuildToBleveQuery(t *testing.T) {
	setField := func(q query.FieldableQuery, f string) query.FieldableQuery {
		q.SetField(f)
		return q
	}

	v100 := float64(100)
	v200 := float64(200)

	tests := []struct {
		fb          *FlexBuild
		expectQuery query.Query
		expectErr   string
	}{
		{},

		{fb: &FlexBuild{Kind: "conjunct"},
			expectQuery: query.NewConjunctionQuery([]query.Query{})},

		{fb: &FlexBuild{Kind: "disjunct"},
			expectQuery: query.NewDisjunctionQuery([]query.Query{})},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "string", `"hello"`},
			},
			expectQuery: setField(query.NewTermQuery("hello"), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "string", `""`},
			},
			expectQuery: setField(query.NewTermQuery(""), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"lt", "a", "string", `"hello"`},
			},
			expectQuery: setField(query.NewTermRangeInclusiveQuery("", "hello", &falseVal, &falseVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"le", "a", "string", `"hello"`},
			},
			expectQuery: setField(query.NewTermRangeInclusiveQuery("", "hello", &falseVal, &trueVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"gt", "a", "string", `"hello"`},
			},
			expectQuery: setField(query.NewTermRangeInclusiveQuery("hello", "", &falseVal, &falseVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"ge", "a", "string", `"hello"`},
			},
			expectQuery: setField(query.NewTermRangeInclusiveQuery("hello", "", &trueVal, &falseVal), "a"),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"eq", "a", "number", `100`},
			},
			expectQuery: setField(query.NewNumericRangeInclusiveQuery(&v100, &v100, &trueVal, &trueVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"lt", "a", "number", `100`},
			},
			expectQuery: setField(query.NewNumericRangeInclusiveQuery(nil, &v100, &falseVal, &falseVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"le", "a", "number", `100`},
			},
			expectQuery: setField(query.NewNumericRangeInclusiveQuery(nil, &v100, &falseVal, &trueVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"gt", "a", "number", `100`},
			},
			expectQuery: setField(query.NewNumericRangeInclusiveQuery(&v100, nil, &falseVal, &falseVal), "a"),
		},
		{
			fb: &FlexBuild{
				Kind: "cmpFieldConstant",
				Data: []string{"ge", "a", "number", `100`},
			},
			expectQuery: setField(query.NewNumericRangeInclusiveQuery(&v100, nil, &trueVal, &falseVal), "a"),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "string", `"hello"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermQuery("hello"), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "string", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "string", `"hello"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermQuery("hello"), "a"),
				setField(query.NewTermQuery("hello"), "b"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "a", "string", `"hello"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"eq", "b", "number", `100`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermQuery("hello"), "a"),
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v100, &trueVal, &trueVal), "b"),
			}),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "b", "number", `100`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "", &falseVal, &falseVal), "a"),
				setField(query.NewNumericRangeInclusiveQuery(nil, &v100, &falseVal, &falseVal), "b"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "b", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "", &falseVal, &falseVal), "a"),
				setField(query.NewTermRangeInclusiveQuery("", "H", &falseVal, &falseVal), "b"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "H", &falseVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "string", `"H"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "H", &falseVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "H", &trueVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"le", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "H", &trueVal, &trueVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"le", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "H", &falseVal, &trueVal), "a"),
			}),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "disjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"A"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewDisjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("A", "", &falseVal, &falseVal), "a"),
				setField(query.NewTermRangeInclusiveQuery("", "H", &falseVal, &falseVal), "a"),
			}),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "b", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, nil, &falseVal, &falseVal), "a"),
				setField(query.NewNumericRangeInclusiveQuery(nil, &v200, &falseVal, &falseVal), "b"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v200, &falseVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "number", `200`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "number", `100`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v200, &falseVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v200, &trueVal, &falseVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"ge", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"le", "a", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v200, &trueVal, &trueVal), "a"),
			}),
		},
		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"le", "a", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(&v100, &v200, &falseVal, &trueVal), "a"),
			}),
		},

		// -----------------------------------------------------------

		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "string", `"C"`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "string", `"H"`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewTermRangeInclusiveQuery("", "C", &falseVal, &falseVal), "a"),
				setField(query.NewTermRangeInclusiveQuery("H", "", &falseVal, &falseVal), "a"),
			}),
		},

		{
			fb: &FlexBuild{
				Kind: "conjunct",
				Children: []*FlexBuild{
					{
						Kind: "cmpFieldConstant",
						Data: []string{"lt", "a", "number", `100`},
					},
					{
						Kind: "cmpFieldConstant",
						Data: []string{"gt", "a", "number", `200`},
					},
				},
			},
			expectQuery: query.NewConjunctionQuery([]query.Query{
				setField(query.NewNumericRangeInclusiveQuery(nil, &v100, &falseVal, &falseVal), "a"),
				setField(query.NewNumericRangeInclusiveQuery(&v200, nil, &falseVal, &falseVal), "a"),
			}),
		},
	}

	for _, test := range tests {
		q, err := FlexBuildToBleveQuery(test.fb, nil)
		if (err != nil) != (test.expectErr != "") {
			t.Fatalf("test: %+v,\n err mismatch, got: %v",
				test, err)
		}

		if !reflect.DeepEqual(q, test.expectQuery) {
			jfb, _ := json.Marshal(test.fb)
			jeq, _ := json.Marshal(test.expectQuery)
			jq, _ := json.Marshal(q)

			t.Errorf("test: %+v,\n jfb: %s\n expectQuery: %s\n query mismatch, got: %s",
				test, jfb, jeq, jq)
		}
	}
}
