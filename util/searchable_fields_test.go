//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package util

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

func TestIndexDefConversion(t *testing.T) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(SampleLandmarkIndexDef, &indexDef)
	if err != nil {
		t.Fatal(err)
	}

	pip, err := ProcessIndexDef(indexDef, "", "")
	if err != nil ||
		pip.SearchFields == nil ||
		len(pip.DynamicMappings) > 0 ||
		pip.DefaultAnalyzer != "standard" ||
		pip.DefaultDateTimeParser != "dateTimeOptional" {
		t.Fatalf("unexpected return values from SearchFieldsForIndexDef")
	}

	expect := map[SearchField]bool{}
	expect[SearchField{Name: "reviews.review", Analyzer: "standard"}] = true
	expect[SearchField{Name: "reviews.review.author", Analyzer: "de", Type: "text"}] = false
	expect[SearchField{Name: "reviews.review.author", Analyzer: "standard", Type: "text"}] = false
	expect[SearchField{Name: "countryX", Analyzer: "standard", Type: "text"}] = false
	expect[SearchField{Name: "reviews.id", Analyzer: "standard", Type: "text"}] = false

	if !reflect.DeepEqual(expect, pip.SearchFields) {
		t.Fatalf("Expected: %v,\n Got: %v", expect, pip.SearchFields)
	}
}

func TestFieldsToSearch(t *testing.T) {
	tests := []struct {
		field  string
		query  value.Value
		expect []string
	}{
		{
			field:  "title",
			query:  value.NewValue(`+Avengers~2 company:marvel`),
			expect: []string{"company", "title"},
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match":     "avengers",
				"field":     "title",
				"fuzziness": 2,
			}),
			expect: []string{"title"},
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match_phrase": "Avengers: Infinity War",
				"field":        "title",
				"analyzer":     "en",
				"boost":        10,
			}),
			expect: []string{"title"},
		},
		{
			field: "title",
			query: value.NewValue(map[string]interface{}{
				"wildcard": "Avengers*",
				"field":    "title",
			}),
			expect: []string{"title"},
		},
		{
			field:  "title",
			query:  value.NewValue(`+movie:Avengers +sequel.id:3 +company:marvel`),
			expect: []string{"company", "movie", "sequel.id", "sequel.id"},
			// Expect 2 sequel.id entries above as the number look up above is
			// considered as a disjunction of a match and a numeric range.
		},
	}

	for _, test := range tests {
		q, err := BuildQuery(test.field, test.query)
		if err != nil {
			t.Fatal(err)
		}
		fieldDescs, err := FetchFieldsToSearchFromQuery(q)
		if err != nil {
			t.Fatal(err)
		}

		fields := []string{}
		for entry := range fieldDescs {
			fields = append(fields, entry.Name)
		}

		sort.Strings(fields)
		if !reflect.DeepEqual(test.expect, fields) {
			t.Fatalf("Expected: %v, Got: %v", test.expect, fields)
		}
	}
}

func TestProcessIndexDef(t *testing.T) {
	tests := []struct {
		about                       string
		indexDef                    string
		expectSearchFields          map[SearchField]bool
		expectCondExpr              string
		expectDynamic               bool
		expectDefaultAnalyzer       string
		expectDefaultDateTimeParser string
		expectErr                   string
	}{
		{
			about: "not a supported indexDef.Type",
			indexDef: `
			{
				"type": "not-a-fulltext-index"
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "a supported indexDef",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"default_analyzer": "cjk",
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"analyzer": "da",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "da"}:       false,
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "a supported indexDef, with docid_prefix",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "docid_prefix",
						"docid_prefix_delim": ":"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_datetime_parser": "dateTimeOptional",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"default_analyzer": "cjk",
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"analyzer": "da",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "da"}:       false,
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              `META().id LIKE "hotel:%"`,
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "a docid_prefix with disallow char is not supported",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "docid_prefix",
						"docid_prefix_delim": "%"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_mapping": {
							"enabled": true,
							"dynamic": true
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "a docid_prefix with disallow char is not supported",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "docid_prefix",
						"docid_prefix_delim": "\""
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_mapping": {
							"enabled": true,
							"dynamic": true
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "a docid_prefix with disallow char is not supported",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "docid_prefix",
						"docid_prefix_delim": "\\"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_mapping": {
							"enabled": true,
							"dynamic": true
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "a supported indexDef, with docid_regexp",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "docid_regexp",
						"docid_regexp": "hotel"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_datetime_parser": "dateTimeOptional",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"analyzer": "keyword",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "keyword"}:  false,
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              `META().id LIKE "%hotel%"`,
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test analyzer inheritance",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_datetime_parser": "crap",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"default_analyzer": "cjk",
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "cjk"}:      false,
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "crap",
			expectErr:                   "",
		},
		{
			about: "test analyzer inheritance, 2",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "super",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "super"}: false,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "super",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test index with no explicit doc-config is a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test index with >1 type-mappings is not a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							},
							"locations": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "standard"}: false,
			},
			expectCondExpr:              "`type` IN [\"hotel\", \"locations\"]",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test when both a default-mapping and a type-mapping exist, it's not a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": true
						},
						"types": {
							"hotel": {
								"enabled": true
							}
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "test when a non-dynamic type-mapping exists with no fields, it's not a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false
							}
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "test when a non-dynamic default-mapping exists with no fields, it's not a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": true,
							"dynamic": false
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "test index with dynamic default-mapping is a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": true,
							"dynamic": true
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "", Type: "", Analyzer: "standard"}: true,
			},
			expectCondExpr:              "",
			expectDynamic:               true,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test when one type-mapping exists that's dynamic, it's a supported FTSIndex",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": true
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "", Type: "", Analyzer: "standard"}: true,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               true,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: "test that disabled type-mapping is ok when there's 1 enabled type-mapping",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": true
							},
							"locations": {
								"enabled": false
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "", Type: "", Analyzer: "standard"}: true,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               true,
			expectDefaultAnalyzer:       "standard",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
		{
			about: `test that a disabled type-mapping is not ok
                    when there's a default mapping (due to false negatives)`,
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"mapping": {
						"default_mapping": {
							"enabled": true
						},
						"types": {
							"hotel": {
								"enabled": false
							}
						}
					}
				}
			}`,
			expectSearchFields:          nil,
			expectCondExpr:              "",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "",
			expectDefaultDateTimeParser: "",
			expectErr:                   "",
		},
		{
			about: "test that multiple fields of the same name are not supported",
			indexDef: `
			{
				"type": "fulltext-index",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "super",
						"default_mapping": {
							"dynamic": true,
							"enabled": false
						},
						"index_dynamic": true,
						"types": {
							"hotel": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										},{
											"name": "country",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			expectSearchFields: map[SearchField]bool{
				{Name: "country", Type: "text", Analyzer: "super"}: false,
			},
			expectCondExpr:              "`type`=\"hotel\"",
			expectDynamic:               false,
			expectDefaultAnalyzer:       "super",
			expectDefaultDateTimeParser: "dateTimeOptional",
			expectErr:                   "",
		},
	}

	for testi, test := range tests {
		var indexDef *cbgt.IndexDef
		err := json.Unmarshal([]byte(test.indexDef), &indexDef)
		if err != nil {
			t.Fatal(err)
		}

		pip, err := ProcessIndexDef(indexDef, "", "")
		if (err != nil) != (test.expectErr != "") {
			t.Fatalf("testi: %d, test: %+v,\n mismatch expectErr, got: %v",
				testi, test, err)
		}

		if !reflect.DeepEqual(test.expectSearchFields, pip.SearchFields) {
			t.Fatalf("testi: %d, test: %+v,\n mismatch searchFields, got: %#v",
				testi, test, pip.SearchFields)
		}

		if len(test.expectCondExpr) > 0 {
			expectCondExpr, err := parser.Parse(test.expectCondExpr)
			if err != nil {
				t.Fatalf("testi: %d, err: %v", testi, err)
			}
			gotCondExpr, err := parser.Parse(pip.CondExpr)
			if err != nil {
				t.Fatalf("testi: %d, err: %v", testi, err)
			}

			expectExprs := expectCondExpr.Children()
			gotExprs := gotCondExpr.Children()

			if len(expectExprs) != len(gotExprs) {
				t.Fatalf("testi: %d, expected condExpr: %s, got: %s",
					testi, expectCondExpr.String(), gotCondExpr.String())
			}

			for i := range expectExprs {
				if len(expectExprs[i].Children()) != len(gotExprs[i].Children()) {
					t.Fatalf("Expect expression: %s, got expression: %s",
						expectExprs[i].String(), gotExprs[i].String())
				}
			}
		}

		if test.expectDynamic != (len(pip.DynamicMappings) > 0) {
			t.Fatalf("testi: %d, test: %+v,\n mismatch dynamic, got: %+v",
				testi, test, pip.DynamicMappings)
		}

		if test.expectDefaultAnalyzer != pip.DefaultAnalyzer {
			t.Fatalf("testi: %d, test: %+v,\n mismatch defaultAnalyzer, got: %+v",
				testi, test, pip.DefaultAnalyzer)
		}

		if test.expectDefaultDateTimeParser != pip.DefaultDateTimeParser {
			t.Fatalf("testi: %d, test: %+v,\n mismatch dateTimeParser, got: %+v",
				testi, test, pip.DefaultDateTimeParser)
		}
	}
}
