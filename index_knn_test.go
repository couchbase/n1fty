//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build vectors
// +build vectors

package n1fty

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/expression/search"
)

func TestSargableFlexWithKNN(t *testing.T) {
	index, err := setupSampleIndex([]byte(`{
		"name": "temp",
		"type": "fulltext-index",
		"sourceName": "temp",
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": true,
					"properties": {
						"desc": {
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "desc",
								"type": "text"
							}
							]
						},
						"txt": {
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "txt",
								"type": "text"
							}
							]
						},
						"vec": {
							"enabled": true,
							"fields": [
							{
								"dims": 5,
								"index": true,
								"name": "vec",
								"similarity": "dot_product",
								"type": "vector"
							}
							]
						}
					}
				}
			},
			"store": {
				"indexType": "scorch",
				"segmentVersion": 16
			}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}

	flexExpr, _ := parser.Parse(`t.txt = "match"`)

	searchExprBytes := []byte(`{
		"query": {
			"match": "odd",
			"field": "desc"
		},
		"knn": [{
			"field": "vec",
			"vector": [0.1, 0.2, 0.3, 0.4, 0.5],
			"k": 3
		}],
		"knn_operator": "and"
	}`)
	var searchExprConst map[string]interface{}
	_ = json.Unmarshal(searchExprBytes, &searchExprConst)

	searchExpr := search.NewSearch(expression.NewConstant(``),
		expression.NewConstant(searchExprConst))

	var expectQuery map[string]interface{}
	_ = json.Unmarshal([]byte(`{"query":{"conjuncts":[{"field":"txt","term":"match"}`+
		`,{"field":"desc","match":"odd","fuzziness":0,"prefix_length":0}]},`+
		`"knn":[{"field":"vec","k":3,"vector":[0.1,0.2,0.3,0.4,0.5]}],`+
		`"knn_operator":"and","score":"none"}`),
		&expectQuery)

	finalExpr := expression.NewAnd(flexExpr, searchExpr)

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     finalExpr,
	}

	resp, n1qlErr := index.SargableFlex("0", flexRequest)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	expectedSargKeys := []string{searchExpr.String(), "txt"}

	if resp == nil || len(resp.StaticSargKeys) != len(expectedSargKeys) {
		t.Fatalf("Resp: %#v", resp)
	}

	for _, key := range expectedSargKeys {
		if resp.StaticSargKeys[key] == nil {
			t.Fatalf("ExpectedSargKeys: %v, Got StaticSargKeys: %v",
				expectedSargKeys, resp.StaticSargKeys)
		}
	}

	var gotQuery map[string]interface{}
	if err := json.Unmarshal([]byte(resp.SearchQuery), &gotQuery); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectQuery, gotQuery) {
		t.Fatalf("Expected query: %v, Got query: %v",
			expectQuery, resp.SearchQuery)
	}
}

// MB-60523
func TestSargableFlexWithKNNFails(t *testing.T) {
	index, err := setupSampleIndex([]byte(`{
		"name": "temp",
		"type": "fulltext-index",
		"sourceName": "temp",
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": true,
					"properties": {
						"vec": {
							"enabled": true,
							"fields": [
							{
								"dims": 5,
								"index": true,
								"name": "vec",
								"similarity": "dot_product",
								"type": "vector"
							}
							]
						}
					}
				}
			},
			"store": {
				"indexType": "scorch",
				"segmentVersion": 16
			}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}

	searchExprBytes := []byte(`{
		"query": {
			"match_none": {}
		},
		"knn": [{
			"field": "vec",
			"vector": [1,2,3],
			"k": 3
		}],
		"knn_operator": "and"
	}`)
	var searchExprConst map[string]interface{}
	_ = json.Unmarshal(searchExprBytes, &searchExprConst)

	searchExpr := search.NewSearch(expression.NewConstant(``),
		expression.NewConstant(searchExprConst))

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     searchExpr,
	}

	resp, n1qlErr := index.SargableFlex("0", flexRequest)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if resp != nil {
		t.Fatalf("Expected resp to be nil as index is NOT sargable - %v", resp)
	}
}

// MB-60523
func TestSargableWithKNNOverDynamicMappings(t *testing.T) {
	searchExprBytes := `{
		"query": {
			"match_none": {}
		},
		"knn": [{
			"field": "vec",
			"vector": [1,2,3],
			"k": 3
		}],
		"knn_operator": "and"
	}`

	searchExpr, err := parser.Parse(searchExprBytes)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		indexDef      []byte
		expectedCount int
	}{
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "temp",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_mapping": {
							"enabled": true,
							"dynamic": true
						}
					},
					"store": {
						"indexType": "scorch",
						"segmentVersion": 16
					}
				}
			}`),
			expectedCount: 0,
		},
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "temp",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_mapping": {
							"enabled": true,
							"dynamic": true,
							"properties": {
								"vec": {
									"enabled": true,
									"fields": [
									{
										"dims": 5,
										"index": true,
										"name": "vec",
										"similarity": "dot_product",
										"type": "vector"
									}
									]
								}
							}
						}
					},
					"store": {
						"indexType": "scorch",
						"segmentVersion": 16
					}
				}
			}`),
			expectedCount: 0,
		},
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "temp",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_mapping": {
							"enabled": true,
							"dynamic": true,
							"properties": {
								"vec": {
									"enabled": true,
									"fields": [
									{
										"dims": 3,
										"index": true,
										"name": "vec",
										"similarity": "dot_product",
										"type": "vector"
									}
									]
								}
							}
						}
					},
					"store": {
						"indexType": "scorch",
						"segmentVersion": 16
					}
				}
			}`),
			expectedCount: 1,
		},
	}

	for i := range tests {
		index, err := setupSampleIndex(tests[i].indexDef)
		if err != nil {
			t.Fatal(err)
		}

		count, _, _, _, _, n1qlErr := index.Sargable("", searchExpr, expression.NewConstant(``), nil)
		if n1qlErr != nil {
			t.Fatal(n1qlErr)
		}

		if count != tests[i].expectedCount {
			t.Errorf("[test-%d] sargable count expected to be %v, but got %v", i+1, tests[i].expectedCount, count)
		}
	}
}

func TestSargabilityOverNamedParametersWithinHybridSearch(t *testing.T) {
	index, err := setupSampleIndex([]byte(`{
		"name": "test",
		"type": "fulltext-index",
		"sourceName": "test",
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
				"dynamic": false,
				"enabled": true,
				"properties": {
					"color": {
						"enabled": true,
						"dynamic": false,
						"fields": [
							{
								"analyzer": "standard",
								"index": true,
								"name": "color",
								"store": true,
								"type": "text"
							}
							]
						},
						"colorvect_l2": {
							"enabled": true,
							"dynamic": false,
							"fields": [
							{
								"dims": 3,
								"index": true,
								"name": "colorvect_l2",
								"similarity": "l2_norm",
								"type": "vector",
								"vector_index_optimized_for": "recall"
							}
							]
						}
					}
				}
			},
			"store": {
				"indexType": "scorch"
			}
		}
	}`))

	if err != nil {
		t.Fatal(err)
	}

	for i, test := range []struct {
		queryStr            string
		expectSargableCount int
	}{
		{
			queryStr:            `{"query": {"match": $x, "field": "color"}}`,
			expectSargableCount: 1,
		},
		{
			// named parameters not supported over KNN queries - because of inability to establish
			// sargability over the vector dimensions without actual data.
			queryStr:            `{"knn": [{"k": 3, "field": "colorvect_l2", "vector":$vec}]}`,
			expectSargableCount: 0,
		},
		{
			queryStr:            `{"query": {"match": "blue", "field": "color"}, "knn": [{"k": 3, "field": "colorvect_l2", "vector":[1,2,3]}]}`,
			expectSargableCount: 2,
		},
		{
			// MB-67744
			queryStr:            `{"query": {"match": $x, "field": "color"}, "knn": [{"k": 3, "field": "colorvect_l2", "vector":[1,2,3]}]}`,
			expectSargableCount: 1,
		},
		{
			queryStr:            `{"knn": [{"filter": {"match": "blue", "field": "color"}, "k": 3, "field": "colorvect_l2", "vector":[1,2,3]}]}`,
			expectSargableCount: 2,
		},
		{
			// MB-67744
			queryStr:            `{"knn": [{"filter": {"match": "blue", "field": "color"}, "k": 3, "field": "colorvect_l2", "vector":$vec}]}`,
			expectSargableCount: 1,
		},
		{
			// MB-67744
			queryStr:            `{"knn": [{"filter": {"match": $x, "field": "color"}, "k": 3, "field": "colorvect_l2", "vector":[1,2,3]}]}`,
			expectSargableCount: 1,
		},
	} {
		queryExpr, err := parser.Parse(test.queryStr)
		if err != nil {
			t.Fatalf("[%d] Failed to parse query expression: %v", i+1, err)
		}

		count, _, _, _, _, n1qlErr := index.Sargable("", queryExpr, nil, nil)
		if n1qlErr != nil {
			t.Fatalf("[%d] Sargable error: %v", i+1, n1qlErr)
		}

		if count != test.expectSargableCount {
			t.Fatalf("[%d] Sargable count expected to be %v, but got %v", i+1, test.expectSargableCount, count)
		}
	}
}
