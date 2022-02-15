//  Copyright (c) 2020 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package n1fty

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
)

func setupSampleIndexOverCollection(scope string, collection string,
	idef []byte) (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(idef, &indexDef)
	if err != nil {
		return nil, err
	}

	pip, err := util.ProcessIndexDef(indexDef, scope, collection)
	if err != nil {
		return nil, err
	}

	if pip.SearchFields != nil || len(pip.DynamicMappings) > 0 {
		return newFTSIndex(nil, indexDef, pip)
	}

	return nil,
		fmt.Errorf("failed to setup index over scope: %v, collection: %v",
			scope, collection)
}

func checkFlexQuerySargability(t *testing.T,
	index *FTSIndex, queryStr, expectQueryStr string) {
	queryExpr, err := parser.Parse(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     queryExpr,
	}

	resp, err := index.SargableFlex("0", flexRequest)
	if err != nil {
		t.Fatal(err)
	}

	if len(expectQueryStr) == 0 {
		if resp != nil {
			t.Fatalf("Expected `%s` to not be sargable for index", queryStr)
		}
		return
	}

	if resp == nil {
		t.Fatalf("Expected `%s` to be sargable for index", queryStr)
	}

	var expectQuery, gotQuery map[string]interface{}
	err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
	if err != nil {
		t.Fatalf("SearchQuery: %s, err: %v", resp.SearchQuery, err)
	}
	err = json.Unmarshal([]byte(expectQueryStr), &expectQuery)
	if err != nil {
		t.Fatalf("ExpectQuery: %s, err: %v", expectQueryStr, err)
	}

	if !reflect.DeepEqual(expectQuery, gotQuery) {
		t.Fatalf("ExpectQuery: %s, GotQuery: %s", expectQueryStr, resp.SearchQuery)
	}
}

// =============================================================================

func TestCollectionIndexSargabilityEntireDefault(t *testing.T) {
	index, err := setupSampleIndexOverCollection("_default", "_default", []byte(`{
		"name": "TestCollectionIndexSargabilityEntireDefault",
		"type": "fulltext-index",
		"sourceName": "default",
		"params": {
			"doc_config": {
				"mode": "scope.collection.type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"_default._default": {
						"default_analyzer": "keyword",
						"dynamic": true,
						"enabled": true
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
	}`))

	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "United States",
		"field": "country",
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 1 || indexedCount != math.MaxInt64 || !exact {
		t.Fatalf("Unexpected results for query: %v, %v, %v",
			count, indexedCount, exact)
	}

	queryStr := `t.country = "United States"`
	expectQueryStr := `{"query":{"term":"United States","field":"country"},"score":"none"}`
	checkFlexQuerySargability(t, index, queryStr, expectQueryStr)
}

func TestCollectionIndexSargabilityDefaultTypeField(t *testing.T) {
	index, err := setupSampleIndexOverCollection("_default", "_default", []byte(`{
		"name": "TestCollectionIndexSargabilityDefaultTypeField",
		"type": "fulltext-index",
		"sourceName": "default",
		"params": {
			"doc_config": {
				"mode": "scope.collection.type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"airline": {
						"dynamic": false,
						"enabled": true,
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
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
	}`))

	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "United States",
		"field": "country",
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 1 || indexedCount != 1 || !exact {
		t.Fatalf("Unexpected results for query: %v, %v, %v",
			count, indexedCount, exact)
	}

	sargableQuery := `t.type = "airline" AND t.country = "United States"`
	expectQueryStr := `{"query":{"term":"United States","field":"country"},"score":"none"}`
	checkFlexQuerySargability(t, index, sargableQuery, expectQueryStr)

	unsargableQuery := `t.type = "airport" AND t.city = "SF"`
	checkFlexQuerySargability(t, index, unsargableQuery, "")
}

func TestCollectionNonSargableIndexDefault(t *testing.T) {
	index, err := setupSampleIndexOverCollection("_default", "_default", []byte(`{
		"name": "TestCollectionNonSargableIndexDefault",
		"type": "fulltext-index",
		"sourceName": "default",
		"params": {
			"doc_config": {
				"mode": "scope.collection.type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"_default._default": {
						"dynamic": true,
						"enabled": true
					},
					"_default._default.airport": {
						"dynamic": true,
						"enabled": true
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
	}`))

	if err == nil || index != nil {
		t.Fatalf("Expected index to NOT be sargable because of index definition" +
			" over entire collection and specific type under collection")
	}
}

func TestCollectionIndexSargabilityTypeField(t *testing.T) {
	index, err := setupSampleIndexOverCollection("scope1", "collection1", []byte(`{
		"name": "TestCollectionIndexSargabilityTypeField",
		"type": "fulltext-index",
		"sourceName": "default",
		"params": {
			"doc_config": {
				"mode": "scope.collection.type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"scope1.collection1.airline": {
						"dynamic": false,
						"enabled": true,
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
					},
					"scope1.collection1.airport": {
						"dynamic": false,
						"enabled": true,
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
					},
					"scope1.collection2.airport": {
						"dynamic": true,
						"enabled": true
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
	}`))

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr       string
		expectQueryStr string
	}{
		{
			queryStr:       `t.type = "airline" AND t.country = "United States"`,
			expectQueryStr: `{"query":{"term":"United States","field":"country"},"score":"none"}`,
		},
		{
			queryStr:       `t.type = "airport" AND t.country = "United States"`,
			expectQueryStr: `{"query":{"term":"United States","field":"country"},"score":"none"}`,
		},
		{
			queryStr:       `(t.type = "airline" OR t.type = "airport") AND t.country = "United States"`,
			expectQueryStr: `{"query":{"term":"United States","field":"country"},"score":"none"}`,
		},
		{
			queryStr:       `t.type IN ["airline", "airport"] AND t.country = "United States"`,
			expectQueryStr: `{"query":{"term":"United States","field":"country"},"score":"none"}`,
		},
		{
			queryStr:       `t.country = "United States"`,
			expectQueryStr: ``,
		},
	}

	for _, test := range tests {
		checkFlexQuerySargability(t, index, test.queryStr, test.expectQueryStr)
	}
}

func TestCollectionIndexSargabilityDocidPrefix(t *testing.T) {
	index, err := setupSampleIndexOverCollection("scope1", "collection1", []byte(`{
		"name": "TestCollectionIndexSargabilityDocidPrefix",
		"type": "fulltext-index",
		"sourceName": "default",
		"params": {
			"doc_config": {
				"mode": "scope.collection.docid_prefix",
				"docid_prefix_delim": "_"
			},
			"mapping": {
				"default_mapping": {
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"scope1.collection1.airline": {
						"dynamic": false,
						"enabled": true,
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
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
	}`))

	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "United States",
		"field": "country",
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 1 || indexedCount != 1 || !exact {
		t.Fatalf("Unexpected results for query: %v, %v, %v",
			count, indexedCount, exact)
	}

	sargableQuery := `meta(t).id LIKE "airline_%" AND t.country = "United States"`
	expectQueryStr := `{"query":{"term":"United States","field":"country"},"score":"none"}`
	checkFlexQuerySargability(t, index, sargableQuery, expectQueryStr)
}
