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
