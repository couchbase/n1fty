//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
	"github.com/couchbase/query/expression/search"
)

func setupSampleIndex(idef []byte) (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(idef, &indexDef)
	if err != nil {
		return nil, err
	}

	pip, err := util.ProcessIndexDef(indexDef, "", "")
	if err != nil {
		return nil, err
	}

	if pip.SearchFields != nil || len(pip.DynamicMappings) > 0 {
		return newFTSIndex(nil, indexDef, pip)
	}

	return nil, fmt.Errorf("failed to setup index")
}

// =============================================================================

func TestBuildQueryWithCtlTimeout(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicDefault)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "blah",
		},
		"ctl": map[string]interface{}{
			"timeout": 3000,
		},
	})

	sargRV := index.buildQueryAndCheckIfSargable("", query.Value(), nil, nil)
	if sargRV.timeoutMS != 3000 {
		t.Fatalf("Expected timeout to be 3000ms, but got: %v", sargRV.timeoutMS)
	}
}

// MB-52476
func TestNilQueryValue(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	// test nil query value.Value
	_ = index.buildQueryAndCheckIfSargable("", nil, nil, nil)
}

func TestIndexSargabilityTypeFieldWithSpecialCharacter(t *testing.T) {
	// For this test we're going to consider an index definition with
	// type_field over a field name with a special character - whitespace.
	if _, err := setupSampleIndex([]byte(`{
		"name": "default",
		"type": "fulltext-index",
		"sourceName": "default",
		"planParams": {
			"indexPartitions": 6
		},
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "Primary Type"
			},
			"mapping": {
				"default_mapping": {
					"dynamic": true,
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"crap": {
						"dynamic": true,
						"enabled": true
					}
				}
			},
			"store": {
				"indexType": "scorch"
			}
		}
	}`)); err != nil {
		t.Fatal(err)
	}
}

func TestIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	field := "`reviews.review.content`"
	query := expression.NewConstant(`countryX:"United" america reviews.id:"10"`)

	count, indexedCount, exact, _, n1qlErr := index.Sargable(field, query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	// The index definition carries:
	// - country
	// - countryX
	// - reviews.review (dynamic)
	// - reviews.id
	// Although the index definition doesn't carry the exact
	// reviews.review.content which is what we are searching for,
	// this query will be sargable as reviews.review is a dynamic mapping.
	if !exact {
		t.Fatalf("Expected the query to be sargable")
	}

	if count != 3 {
		t.Fatalf("Expected sargable count of 3, but got: %v", count)
	}

	if indexedCount != math.MaxInt64 {
		t.Fatalf("Expected indexed count of MaxInt64, but got: %v", indexedCount)
	}
}

func TestIndexSargabilityWithSearchRequest(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	field := ""
	query := expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"Size":    10,
		"From":    0,
		"Explain": false})

	count, indexedCount, exact, _, n1qlErr := index.Sargable(field, query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if !exact {
		t.Fatalf("Expected the query to be sargable")
	}

	if count != 1 {
		t.Fatalf("Expected sargable_count of 1, but got count: %v", count)
	}

	if indexedCount != math.MaxInt64 {
		t.Fatalf("Expected indexed count of MaxInt64, but got: %v", indexedCount)
	}
}

func TestDynamicIndexSargabilityWithIncompatibleAnalyzer(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicDefault)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match":    "United States",
		"field":    "country",
		"analyzer": "keyword",
	})

	count, _, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 0 {
		t.Fatalf("Expected count of 0, as query is not sargable for index,"+
			" but got: %v", count)
	}
}

func TestDynamicDefaultIndexSargabilityNoFieldsQuery(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicDefault)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"query": "california",
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != math.MaxInt64 || indexedCount != math.MaxInt64 || !exact {
		t.Fatal("Unexpected results from Index.Sargable(...) for query")
	}
}

func TestCustomIndexSargabilityNoFieldsQuery(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithCustomDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "san francisco",
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 3 || indexedCount != 3 || !exact {
		t.Fatal("Unexpected results from Index.Sargable(...) for query")
	}
}

func TestIncompatibleIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match":    "United States",
		"field":    "country",
		"analyzer": "default",
	})

	optBytes := []byte(`
	{
		"index": {
			"default_analyzer": "standard",
			"type_field": "_type",
			"default_mapping": {
				"dynamic": true,
				"enabled": false
			},
			"types": {
				"hotel": {
					"enabled": true,
					"dynamic": false,
					"properties": {
						"city": {
							"enabled": true,
							"dynamic": false,
							"fields": [{
								"name": "city",
								"type": "text",
								"store": false,
								"index": true
							}]
						}
					}
				}
			}
		}
	}
	`)

	var opt map[string]interface{}
	err = json.Unmarshal(optBytes, &opt)
	if err != nil {
		t.Fatal(err)
	}

	count, _, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(opt), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 0 {
		t.Fatalf("Expected count of 0, as query is not sargable for index," +
			" on grounds that provided mapping in options isn't compatible" +
			" with the query's fields.")
	}
}

func TestCompatibleIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match":    "United States",
		"field":    "countryX",
		"analyzer": "standard",
	})

	optBytes := []byte(`
	{
		"index": {
			"default_analyzer": "standard",
			"default_mapping": {
				"dynamic": true,
				"enabled": false
			},
			"types": {
				"landmark": {
					"enabled": true,
					"dynamic": false,
					"properties": {
						"country": {
							"enabled": true,
							"dynamic": false,
							"fields": [{
								"name": "countryX",
								"type": "text",
								"store": false,
								"index": true
							}]
						}
					}
				}
			}
		}
	}
	`)

	var opt map[string]interface{}
	err = json.Unmarshal(optBytes, &opt)
	if err != nil {
		t.Fatal(err)
	}

	count, _, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(opt), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 1 {
		t.Fatalf("Expected sargable_count of 1, because query and custom"+
			" mapping should be sargable for the index, but got count: %v",
			count)
	}
}

func TestIndexSargabilityCompatibleCustomDefaultMapping(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithCustomDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	queBytes := []byte(`
	{
		"conjuncts":[{
			"match": "United States",
			"field": "country",
			"analyzer": "keyword"
		}, {
			"match": "San Francisco",
			"field": "city"
		}]
	}
	`)
	var que map[string]interface{}
	err = json.Unmarshal(queBytes, &que)
	if err != nil {
		t.Fatal(err)
	}

	optBytes := []byte(`
	{
		"index": {
			"default_mapping": {
				"enabled": true,
				"dynamic": false,
				"properties": {
					"city": {
                        "dynamic": false,
						"fields": [{
							"name": "city", "type": "text", "index": true
						}]
					},
					"country": {
                        "dynamic": false,
						"fields": [{
							"name": "country", "type": "text", "index": true, "analyzer": "keyword"
						}]
					}
				}
			},
			"default_type": "_default",
			"default_analyzer": "standard",
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			}
		}
	}
	`)
	var opt map[string]interface{}
	err = json.Unmarshal(optBytes, &opt)
	if err != nil {
		t.Fatal(err)
	}

	count, indexedCount, exact, _, n1qlErr := index.Sargable("",
		expression.NewConstant(que), expression.NewConstant(opt), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if !exact {
		t.Fatalf("Expected the query to be sargable")
	}

	if count != 2 {
		t.Fatalf("Expected sargable count of 2, but got: %v", count)
	}

	if indexedCount != 3 {
		t.Fatalf("Expected indexed count of 2, but got: %v", indexedCount)
	}
}

func TestIndexPageable(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	// missing pagination info (size) in search request
	query := expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"from":    0,
		"explain": false,
		"sort":    []interface{}{"-_score"},
	})

	pageable := index.Pageable([]string{"score DESC"}, 0, 10, query,
		expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

	// missing pagination info (from) in search request
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    10,
		"explain": false,
		"sort":    []interface{}{"country", "city", "-_score"},
	})

	expOrder := []string{"country", "city", "-_score"}
	order := []string{"country", "city", "-_score"}

	pageable = index.Pageable(order, 0,
		10, query, expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

	if !reflect.DeepEqual(order, expOrder) {
		t.Fatalf("order got changed, expected: %v, but got: %v", expOrder, order)
	}

	// pagination given correctly in search request
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    100,
		"from":    10,
		"explain": false,
	})

	pageable = index.Pageable(nil, 0,
		10, query, expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

	// pagination given with Sort in the search request
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    100,
		"from":    10,
		"explain": false,
		"sort":    []interface{}{"country", "city", "-_score"},
	})

	pageable = index.Pageable([]string{"country", "city", "-_score"}, 0,
		10, query, expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

}

func TestIndexSargabilityOverDateTimeFields(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithCustomDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"start":           "2019-03-25 12:00:00",
		"inclusive_start": true,
		"field":           "currentTime",
		"end":             "2019-03-25 12:00:00",
		"inclusive_end":   true,
	})

	count, indexedCount, exact, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 1 || indexedCount != 3 || !exact {
		t.Fatal("Unexpected results from Index.Sargable(...) for query")
	}
}

func TestIndexSargabilityInvalidIndexName(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicDefault)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "california",
	})

	options := expression.NewConstant(map[string]interface{}{
		"index": "wrong_name",
	})

	count, indexedCount, _, _, n1qlErr := index.Sargable("", query,
		options, nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 0 || indexedCount != 0 {
		t.Fatal("Unexpected results from Index.Sargable(...) for query")
	}
}

func TestIndexSargabilityForQueryWithMissingAnalyzer(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithCustomDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	// Note that the index has the field "country" indexed under the
	// "keyword" analyzer.
	query := expression.NewConstant(map[string]interface{}{
		"prefix": "blah",
		"field":  "country",
	})

	count, indexedCount, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if indexedCount != 3 {
		t.Fatalf("Expected indexedCount of 3 but got: %v", indexedCount)
	}

	if count != 1 {
		t.Fatalf("Expected count of 1, as query is not sargable for index,"+
			" but got: %v", count)
	}
}

func TestIndexSargabilityNoAllField(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithNoAllField)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match": "san francisco",
	})

	count, _, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 0 {
		t.Fatal("Expect index to NOT be sargable")
	}
}

// =============================================================================

func TestNotSargableFlexIndex(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	for _, queryStr := range []string{
		`t.country = 'United States'`,
		`t.type <= hotel`,
		`t.type > hotel`,
		`t.id >= 10`,
		`t.id < 10`,
		`t.isOpen > true`,
		`t.isOpen < false`,
		`t.createdOn > '1985-04-12T23:20:50.52Z'`,
		`t.createdOn <= '2020-01-30T12:00:00.00Z'`,
		`t.id < 10 AND t.id > 0`,                              // min expression to be specified first
		`t.type < "hotel" AND t.type > "hot"`,                 // min expression to specified first
		`t.createdOn >= 'asdasd' AND t.createdOn <= "dsadsa"`, // createdOn: incorrect datatype
		`t.id = 10 OR t.createdOn = 'asdasd'`,                 // createdOn: incorrect datatype
	} {
		queryExp, err := parser.Parse(queryStr)
		if err != nil {
			t.Fatal(err)
		}

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExp,
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if resp != nil {
			t.Fatalf("Expected query: `%s` to be NOT-SARGABLE, got resp: %#v",
				queryStr, resp)
		}
	}
}

func testQueryOverFlexIndex(t *testing.T, index *FTSIndex,
	queryStr, expectedQueryStr string, expectedSargKeys []string) {
	queryExpression, err := parser.Parse(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     queryExpression,
	}

	resp, err := index.SargableFlex("0", flexRequest)
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil || len(resp.StaticSargKeys) != len(expectedSargKeys) {
		t.Fatalf("Query: %v, Resp: %#v", queryStr, resp)
	}

	for _, key := range expectedSargKeys {
		if resp.StaticSargKeys[key] == nil {
			t.Fatalf("ExpectedSargKeys: %v, Got StaticSargKeys: %v",
				expectedSargKeys, resp.StaticSargKeys)
		}
	}

	var gotQuery, expectedQuery map[string]interface{}
	err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
	if err != nil {
		t.Fatalf("SearchQuery: %s, err: %v", resp.SearchQuery, err)
	}
	err = json.Unmarshal([]byte(expectedQueryStr), &expectedQuery)
	if err != nil {
		t.Fatalf("ExpectedQuery: %s, err: %v", expectedQueryStr, err)
	}

	if !reflect.DeepEqual(expectedQuery, gotQuery) {
		t.Fatalf("ExpectedQuery: %s, GotQuery: %s", expectedQueryStr, resp.SearchQuery)
	}
}

func TestSargableFlexIndex(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
		expectedSargKeys []string
	}{
		{
			queryStr:         "t.type = 'hotel' AND t.country = 'United States'",
			expectedQueryStr: `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.type >= 'hot' AND t.type <= 'hotel'",
			expectedQueryStr: `{"query": {"field":"type","min":"hot","inclusive_min":true,` +
				`"max":"hotel","inclusive_max":true},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.type = 'hotel' AND t.id = 10",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
				`{"field":"id","inclusive_max":true,"inclusive_min":true,"max":10,` +
				`"min":10}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "id"},
		},
		{
			queryStr: "t.type = 'hotel' AND (t.id = 10 OR t.id = 20)",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
				`{"disjuncts":[{"field":"id","inclusive_max":true,"inclusive_min":true,` +
				`"max":10,"min":10},{"field":"id","inclusive_max":true,` +
				`"inclusive_min":true,"max":20,"min":20}]}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "id"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id < 10",
			expectedQueryStr: `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id <= 10",
			expectedQueryStr: `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id > 10",
			expectedQueryStr: `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id >= 10",
			expectedQueryStr: `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.id >= 0 and t.id < 20",
			expectedQueryStr: `{"query":{"field":"id","min":0,"inclusive_min":true,` +
				`"max":20,"inclusive_max":false},"score":"none"}`,
			expectedSargKeys: []string{"id"},
		},
		{
			queryStr: "t.id >= -20 and t.id <= -5",
			expectedQueryStr: `{"query":{"field":"id","min":-20,"inclusive_min":true,` +
				`"max":-5,"inclusive_max":true},"score":"none"}`,
			expectedSargKeys: []string{"id"},
		},
		{
			queryStr: "t.type = 'hotel' AND t.id > 5 AND t.id < 10",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
				`{"field":"id","min":5,"inclusive_min":false,"max":10,` +
				`"inclusive_max":false}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "id"},
		},
		{
			queryStr: "t.isOpen = true AND t.type = 'hotel'",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"isOpen","bool":true},` +
				`{"field":"type","term":"hotel"}]},"score":"none"}`,
			expectedSargKeys: []string{"isOpen", "type"},
		},
		{
			queryStr: "t.type = 'hotel' AND t.createdOn = '1985-04-12T23:20:50.52Z'",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"type","term": "hotel"},` +
				`{"field":"createdOn","start":"1985-04-12T23:20:50.52Z",` +
				`"inclusive_start":true,"end":"1985-04-12T23:20:50.52Z",` +
				`"inclusive_end":true}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "createdOn"},
		},
		{
			queryStr: "t.createdOn > '1985-04-12T23:20:50.52Z'" +
				"AND t.createdOn <= '2020-01-30T12:00:00.00Z'",
			expectedQueryStr: `{"query":{"field":"createdOn",` +
				`"start":"1985-04-12T23:20:50.52Z","inclusive_start":false,` +
				`"end":"2020-01-30T12:00:00.00Z","inclusive_end":true},"score":"none"}`,
			expectedSargKeys: []string{"createdOn"},
		},
		{
			// this is a "datetime" search term over a "text" indexed field,
			// query treated as sargable - although no results may be returned.
			queryStr: "t.type = '1985-04-12T23:20:50.52Z'",
			expectedQueryStr: `{"query":{"field":"type",` +
				`"term":"1985-04-12T23:20:50.52Z"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			// createdOn is indexed as "datetime", so this query is partially sargable.
			queryStr: "t.type = 'hotel' AND t.createdOn = 'crap'",
			expectedQueryStr: `{"query":{"field":"type",` +
				`"term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.type IN ['airline', 'airport'] AND t.createdOn = '2020-05-18'",
			expectedQueryStr: `{"query":{"conjuncts":[{"disjuncts":[{"field":"type",` +
				`"term":"airline"},{"field":"type","term":"airport"}]},` +
				`{"field":"createdOn","start":"2020-05-18","end":"2020-05-18",` +
				`"inclusive_start":true,"inclusive_end":true}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "createdOn"},
		},
	}

	for i := range tests {
		testQueryOverFlexIndex(t, index, tests[i].queryStr, tests[i].expectedQueryStr,
			tests[i].expectedSargKeys)
	}
}

func TestSargableDynamicFlexIndex(t *testing.T) {
	// Only a default dynamic index with "default_analzyer" set to "keyword"
	index, err := setupSampleIndex(
		util.SampleIndexDefDynamicWithAnalyzerKeyword)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
		expectedSargKeys []string
	}{
		{
			queryStr:         "t.country = 'United States'",
			expectedQueryStr: `{"query":{"field":"country","term":"United States"},"score":"none"}`,
			expectedSargKeys: []string{"country"},
		},
		{
			queryStr: "t.type >= 'hot' AND t.type <= 'hotel'",
			expectedQueryStr: `{"query": {"field":"type","min":"hot","inclusive_min":true,` +
				`"max":"hotel","inclusive_max":true},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.id = 10",
			expectedQueryStr: `{"query":{"field":"id","inclusive_max":true,"inclusive_min":true,` +
				`"max":10,"min":10},"score":"none"}`,
			expectedSargKeys: []string{"id"},
		},
		{
			queryStr: "t.id >= 0 AND t.id < 20",
			expectedQueryStr: `{"query":{"field":"id","min":0,"inclusive_min":true,` +
				`"max":20,"inclusive_max":false},"score":"none"}`,
			expectedSargKeys: []string{"id"},
		},
		{
			queryStr: "t.id > -25 AND t.id <= 10",
			expectedQueryStr: `{"query":{"field":"id","min":-25,"inclusive_min":false,` +
				`"max":10,"inclusive_max":true},"score":"none"}`,
			expectedSargKeys: []string{"id"},
		},
		{
			queryStr:         "t.isOpen = true",
			expectedQueryStr: `{"query":{"field":"isOpen","bool":true},"score":"none"}`,
			expectedSargKeys: []string{"isOpen"},
		},
		{
			queryStr: "t.createdOn = '1985-04-12T23:20:50.52Z'",
			expectedQueryStr: `{"query":{"field":"createdOn","start":"1985-04-12T23:20:50.52Z",` +
				`"inclusive_start":true,"end":"1985-04-12T23:20:50.52Z","inclusive_end":true},` +
				`"score":"none"}`,
			expectedSargKeys: []string{"createdOn"},
		},
		{
			queryStr: "t.createdOn > '1985-04-12T23:20:50.52Z'" +
				"AND t.createdOn <= '2020-01-30T12:00:00.00Z'",
			expectedQueryStr: `{"query":{"field":"createdOn",` +
				`"start":"1985-04-12T23:20:50.52Z","inclusive_start":false,` +
				`"end":"2020-01-30T12:00:00.00Z","inclusive_end":true},"score":"none"}`,
			expectedSargKeys: []string{"createdOn"},
		},
		{
			queryStr: "t.createdOn IN ['2020-05-18', '2020-05-19']",
			expectedQueryStr: `{"query":{"disjuncts":[{"field":"createdOn","start":"2020-05-18",` +
				`"end":"2020-05-18","inclusive_start":true,"inclusive_end":true},` +
				`{"field":"createdOn","start":"2020-05-19","end":"2020-05-19",` +
				`"inclusive_start":true,"inclusive_end":true}]},"score":"none"}`,
			expectedSargKeys: []string{"createdOn"},
		},
		{
			queryStr: "t.id IN [10] AND t.type = 'hotel'",
			expectedQueryStr: `{"query":{"conjuncts":[{"field":"id","min":10,"max":10,` +
				`"inclusive_min":true,"inclusive_max":true},{"field":"type","term":"hotel"}]}` +
				`,"score":"none"}`,
			expectedSargKeys: []string{"id", "type"},
		},
	}

	for i := range tests {
		testQueryOverFlexIndex(t, index, tests[i].queryStr, tests[i].expectedQueryStr,
			tests[i].expectedSargKeys)
	}
}

func TestComplexQuerySargabilityOverFlexIndexes(t *testing.T) {
	indexes := [][]byte{
		util.SampleIndexDefWithSeveralNestedFieldsUnderDefaultMapping,
		util.SampleIndexDefDynamicWithAnalyzerKeyword,
		util.SampleIndexDefWithSeveralNestedFieldsUnderHotelMapping,
	}

	for i := range indexes {
		index, err := setupSampleIndex(indexes[i])
		if err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			queryStr                       string
			expectedQueryStrDefaultMapping string
			expectedQueryStrCustomMapping  string
			expectedSargKeys               []string
		}{
			{
				queryStr: `t.type = "hotel"` +
					` AND ANY r in t.reviews SATISFIES r.author = "Shaniya Wisoky" END`,
				expectedQueryStrDefaultMapping: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
					`{"field":"reviews.author","term":"Shaniya Wisoky"}]},"score":"none"}`,
				expectedQueryStrCustomMapping: `{"query":{"field":"reviews.author",` +
					`"term":"Shaniya Wisoky"},"score":"none"}`,
				expectedSargKeys: []string{"type", "reviews.author"},
			},
			{
				queryStr: `t.type = "hotel"` +
					` AND ANY r IN t.reviews SATISFIES r.ratings.Cleanliness = 5 OR r.ratings.Overall = 4 END` +
					` AND ANY p IN t.public_likes SATISFIES p LIKE "Raymundo Quigley" END`,
				expectedQueryStrDefaultMapping: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
					`{"disjuncts":[{"field":"reviews.ratings.Cleanliness","inclusive_max":true,` +
					`"inclusive_min":true,"max":5,"min":5},{"field":"reviews.ratings.Overall",` +
					`"inclusive_max":true,"inclusive_min":true,"max":4,"min":4}]},` +
					`{"field":"public_likes","wildcard":"Raymundo Quigley"}]},"score":"none"}`,
				expectedQueryStrCustomMapping: `{"query":{"conjuncts":[{"disjuncts":[` +
					`{"field":"reviews.ratings.Cleanliness",` +
					`"inclusive_max":true,"inclusive_min":true,"max":5,"min":5},` +
					`{"field":"reviews.ratings.Overall","inclusive_max":true,"inclusive_min":true,` +
					`"max":4,"min":4}]},{"field":"public_likes","wildcard":"Raymundo Quigley"}]},"score":"none"}`,
				expectedSargKeys: []string{
					"type", "reviews.ratings.Cleanliness", "reviews.ratings.Overall", "public_likes",
				},
			},
		}

		for j := range tests {
			queryExpression, err := parser.Parse(tests[j].queryStr)
			if err != nil {
				t.Fatal(tests[j].queryStr, err)
			}

			flexRequest := &datastore.FTSFlexRequest{
				Keyspace: "t",
				Pred:     queryExpression,
			}

			resp, err := index.SargableFlex("0", flexRequest)
			if err != nil {
				t.Fatal(err)
			}

			expectedQueryStr := ""

			if index.condExpr == nil {
				// default mapping in use
				expectedQueryStr = tests[j].expectedQueryStrDefaultMapping
			} else {
				expectedQueryStr = tests[j].expectedQueryStrCustomMapping
			}

			if resp == nil || len(resp.StaticSargKeys) != len(tests[j].expectedSargKeys) {
				t.Fatalf("[%s] Query: %v, Resp: %#v", index.Name(), tests[j].queryStr, resp)
			}

			for _, key := range tests[j].expectedSargKeys {
				if resp.StaticSargKeys[key] == nil {
					t.Fatalf("[%s] ExpectedSargKeys: %v, Got StaticSargKeys: %v",
						index.Name(), tests[j].expectedSargKeys, resp.StaticSargKeys)
				}
			}

			var gotQuery, expectedQuery map[string]interface{}
			err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
			if err != nil {
				t.Fatalf("[%s] SearchQuery: %s, err: %v", index.Name(), resp.SearchQuery, err)
			}
			err = json.Unmarshal([]byte(expectedQueryStr), &expectedQuery)
			if err != nil {
				t.Fatalf("[%s] ExpectedQuery: %s, err: %v", index.Name(), expectedQueryStr, err)
			}

			if !reflect.DeepEqual(expectedQuery, gotQuery) {
				t.Fatalf("[%s] ExpectedQuery: %s, GotQuery: %s", index.Name(), expectedQueryStr,
					resp.SearchQuery)
			}
		}
	}
}

// =============================================================================

func TestIndexMultipleTypeMappings(t *testing.T) {
	index, err := setupSampleIndex([]byte(`{
		"name": "default",
		"type": "fulltext-index",
		"sourceName": "default",
		"planParams": {
			"indexPartitions": 6
		},
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"dynamic": true,
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"type1": {
						"dynamic": true,
						"enabled": true
					},
					"type2": {
						"dynamic": true,
						"enabled": true
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

	expectCondExprStr := "`type` IN [\"type1\", \"type2\"]"
	expectCondExpr, err := parser.Parse(expectCondExprStr)
	if err != nil {
		t.Fatal(err)
	}

	expectExprs := expectCondExpr.Children()
	gotExprs := index.condExpr.Children()

	if len(expectExprs) != len(gotExprs) {
		t.Fatalf("Expected condExpr: %s, got: %s",
			expectCondExpr.String(), index.condExpr.String())
	}

	for i := range expectExprs {
		if len(expectExprs[i].Children()) != len(gotExprs[i].Children()) {
			t.Fatalf("Expect expression: %s, got expression: %s",
				expectExprs[i].String(), gotExprs[i].String())
		}
	}
}

func TestSargableFlexIndexWithMultipleTypeMappings(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithMultipleTypeMappings)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
	}{
		{
			queryStr:         `t.type = "airline" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `t.type = "airport" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `(t.type = "airline" OR t.type = "airport") AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `t.type = "airline" OR t.type = "airport" AND t.country = "US"`,
			expectedQueryStr: ``,
		},
		{
			queryStr:         `t.type = "airline" OR (t.type = "airport" AND t.country = "US")`,
			expectedQueryStr: ``,
		},
		{
			queryStr:         `(t.type = "airport") OR (t.type = "airline" AND t.country = "US")`,
			expectedQueryStr: ``,
		},
		{
			// Expect this expression to be sargable, although filtering (non-covering index)
			// will ensure that no results are returned.
			queryStr:         `t.type = "airport" AND t.type = "airline" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// No "type" expressions provided.
			queryStr:         `(t.city = "airline" OR t.city = "airport") AND t.country = "US"`,
			expectedQueryStr: ``,
		},
		{
			queryStr:         `t.type IN ["airline"] AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `t.type IN ["airline", "airport"] AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `t.country = "US" AND t.type IN ["airline", "airport"]`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// "type" expression(s) not searchable.
			queryStr:         `t.type IN ["airline", "airport"]`,
			expectedQueryStr: ``,
		},
		{
			// No "type" expressions provided.
			queryStr:         `t.city IN ["airline", "airport"] AND t.country = "US"`,
			expectedQueryStr: ``,
		},
		{
			// MB-39517: "city" not indexed within "airline" type mapping
			queryStr:         `t.type = "airline" AND t.city = "SF"`,
			expectedQueryStr: ``,
		},
		{
			// MB-39517: "city" indexed within "airport" type mapping
			queryStr:         `t.type = "airport" AND t.city = "SF"`,
			expectedQueryStr: `{"query":{"field":"city","term":"SF"},"score":"none"}`,
		},
		{
			// MB-39517: "city" only indexed within "airport" type mapping
			queryStr:         `(t.type = "airline" OR t.type = "airport") AND t.city = "SF"`,
			expectedQueryStr: ``,
		},
		{
			// MB-39517: "city" only indexed within "airport" type mapping
			queryStr:         `t.type IN ["airline", "airport"] AND t.city = "SF"`,
			expectedQueryStr: ``,
		},
		{
			// MB-39517: "city" not indexed within "airline", but country is within both, so partially sargable
			queryStr:         `t.type IN ["airline", "airport"] AND t.country = "US" AND t.city = "SF"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// MB-39517: "city" not indexed within "airline", but country is within both, so partially sargable
			queryStr:         `(t.type = "airline" OR t.type = "airport") AND t.country = "US" AND t.city = "SF"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// MB-39517: "city" not indexed within "airline", although country is within both, it's a disjunction
			queryStr:         `t.type IN ["airline", "airport"] AND t.country = "US" OR t.city = "SF"`,
			expectedQueryStr: ``,
		},
		{
			// MB-39517: "city" not indexed within "airline", although country is within both, it's a disjunction
			queryStr:         `(t.type = "airline" OR t.type = "airport") AND t.country = "US" OR t.city = "SF"`,
			expectedQueryStr: ``,
		},
	}

	for i := range tests {
		queryExpression, err := parser.Parse(tests[i].queryStr)
		if err != nil {
			t.Fatal(tests[i].queryStr, err)
		}

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExpression,
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if len(tests[i].expectedQueryStr) == 0 {
			if resp != nil {
				t.Errorf("[%d] Expected `%s` to not be sargable for index",
					i, tests[i].queryStr)
			}
			continue
		}

		if resp == nil {
			t.Errorf("[%d] Expected `%s` to be sargable for index", i, tests[i].queryStr)
			continue
		}

		var gotQuery, expectedQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Errorf("[%d] SearchQuery: %s, err: %v", i, resp.SearchQuery, err)
		}
		err = json.Unmarshal([]byte(tests[i].expectedQueryStr), &expectedQuery)
		if err != nil {
			t.Errorf("[%d] ExpectedQuery: %s, err: %v", i, tests[i].expectedQueryStr, err)
		}

		if !reflect.DeepEqual(expectedQuery, gotQuery) {
			t.Errorf("[%d] ExpectedQuery: %s, GotQuery: %s",
				i, tests[i].expectedQueryStr, resp.SearchQuery)
		}
	}
}

func TestSargableFlexIndexDocIDPrefixWithMultipleTypeMappings(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithDocIdPrefixMultipleTypeMappings)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
	}{
		{
			queryStr:         `meta(t).id LIKE "airline-%" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `meta(t).id LIKE "airport-%" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `(meta(t).id LIKE "airline-%" OR meta(t).id LIKE "airport-%") AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// MB-39517: "city" indexed within "airport" type mapping
			queryStr:         `meta(t).id LIKE "airport-%" AND t.city = "SF"`,
			expectedQueryStr: `{"query":{"field":"city","term":"SF"},"score":"none"}`,
		},
		{
			// MB-39517: "city" only indexed within "airport" type mapping
			queryStr:         `meta(t).id LIKE "airline-%" AND t.city = "SF"`,
			expectedQueryStr: ``,
		},
	}

	for i := range tests {
		queryExpression, err := parser.Parse(tests[i].queryStr)
		if err != nil {
			t.Fatal(tests[i].queryStr, err)
		}

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExpression,
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if len(tests[i].expectedQueryStr) == 0 {
			if resp != nil {
				t.Errorf("[%d] Expected `%s` to not be sargable for index",
					i, tests[i].queryStr)
			}
			continue
		}

		if resp == nil {
			t.Errorf("[%d] Expected `%s` to be sargable for index", i, tests[i].queryStr)
			continue
		}

		var gotQuery, expectedQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Errorf("[%d] SearchQuery: %s, err: %v", i, resp.SearchQuery, err)
		}
		err = json.Unmarshal([]byte(tests[i].expectedQueryStr), &expectedQuery)
		if err != nil {
			t.Errorf("[%d] ExpectedQuery: %s, err: %v", i, tests[i].expectedQueryStr, err)
		}

		if !reflect.DeepEqual(expectedQuery, gotQuery) {
			t.Errorf("[%d] ExpectedQuery: %s, GotQuery: %s",
				i, tests[i].expectedQueryStr, resp.SearchQuery)
		}
	}
}

func TestSargableFlexIndexWithLikeExpressions(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicWithAnalyzerKeyword)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
	}{
		{
			queryStr:         `t.type LIKE "ho%l"`,
			expectedQueryStr: `{"query":{"field":"type","wildcard":"ho*l"},"score":"none"}`,
		},
		{
			queryStr:         `t.type LIKE "hot_l"`,
			expectedQueryStr: `{"query":{"field":"type","wildcard":"hot?l"},"score":"none"}`,
		},
		{
			queryStr:         `t.type LIKE "%t%"`,
			expectedQueryStr: `{"query":{"field":"type","wildcard":"*t*"},"score":"none"}`,
		},
		{
			queryStr:         `t.type LIKE "\\%@hot_l%"`,
			expectedQueryStr: `{"query":{"field":"type","wildcard":"\\%@hot?l*"},"score":"none"}`,
		},
	}

	for i := range tests {
		queryExpression, err := parser.Parse(tests[i].queryStr)
		if err != nil {
			t.Fatal(tests[i].queryStr, err)
		}

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExpression,
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if len(tests[i].expectedQueryStr) == 0 {
			if resp != nil {
				t.Errorf("[%d] Expected `%s` to not be sargable for index, but got: %v",
					i, tests[i].queryStr, resp)
			}
			continue
		}

		if resp == nil {
			t.Errorf("[%d] Expected `%s` to be sargable for index", i, tests[i].queryStr)
			continue
		}

		var gotQuery, expectedQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Errorf("[%d] SearchQuery: %s, err: %v", i, resp.SearchQuery, err)
			continue
		}
		err = json.Unmarshal([]byte(tests[i].expectedQueryStr), &expectedQuery)
		if err != nil {
			t.Errorf("[%d] ExpectedQuery: %s, err: %v", i, tests[i].expectedQueryStr, err)
			continue
		}

		if !reflect.DeepEqual(expectedQuery, gotQuery) {
			t.Errorf("[%d] ExpectedQuery: %s, GotQuery: %s",
				i, tests[i].expectedQueryStr, resp.SearchQuery)
		}
	}
}

func TestFlexPushDownForLimitOffsetOrderOverId(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	queryStr := `t.type = "hotel" AND t.id >= 10 AND t.id <= 20`
	queryExpr, _ := parser.Parse(queryStr)
	sortOnID, _ := parser.Parse("t.id")
	flexRequest := &datastore.FTSFlexRequest{
		Keyspace:      "t",
		Pred:          queryExpr,
		CheckPageable: true,
		Order: []*datastore.SortTerm{
			{
				Expr:       sortOnID,
				Descending: true,
				NullsPos:   datastore.ORDER_NULLS_NONE,
			},
		},
		Offset: 5,
		Limit:  20,
	}

	resp, err := index.SargableFlex("0", flexRequest)
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil {
		t.Fatalf("Expected a response, but didn't get any")
	}

	var expectQuery, gotQuery map[string]interface{}
	_ = json.Unmarshal([]byte(`{"query":{"conjuncts":[{"field":"type","term":"hotel"},`+
		`{"field":"id","inclusive_max":true,"inclusive_min":true,"max":20,`+
		`"min":10}]},"score":"none", "size": 20, "from": 5, "sort": ["-id"]}`),
		&expectQuery)
	if err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectQuery, gotQuery) {
		t.Fatalf("Expected query: %v, Got Query: %v", expectQuery, gotQuery)
	}
}

func TestFlexPushDownOrderOverField(t *testing.T) {
	queryStr := `t.type = "XYZ"`
	queryExp, err := parser.Parse(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	orderBy := `t.name`
	orderByExp, err := parser.Parse(orderBy)
	if err != nil {
		t.Fatal(err)
	}

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace:      "t",
		Pred:          queryExp,
		CheckPageable: true,
		Order: []*datastore.SortTerm{
			&datastore.SortTerm{Expr: orderByExp, NullsPos: datastore.ORDER_NULLS_NONE},
		},
		Limit: 5,
	}

	expectSearchQuery := `{"from":0,"query":{"field":"type","term":"XYZ"},` +
		`"score":"none","size":5,"sort":["name"]}`
	var expectQuery map[string]interface{}
	_ = json.Unmarshal([]byte(expectSearchQuery), &expectQuery)

	for i, indexDef := range [][]byte{
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping,
		util.SampleIndexDefDynamicWithAnalyzerKeyword,
	} {
		index, err := setupSampleIndex(indexDef)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if resp == nil {
			t.Fatalf("Expected query to be SARGABLE for index-%d", i+1)
		}

		var gotQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(gotQuery, expectQuery) {
			t.Fatalf("Unexpected search query for index-%d, %s", i+1, resp.SearchQuery)
		}
	}
}
func TestFlexPushDownSearchFunc1(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefDynamicWithAnalyzerKeyword)
	if err != nil {
		t.Fatal(err)
	}

	flexExpr, _ := parser.Parse(`t.country = "United States"`)

	tests := []struct {
		searchExpr expression.Expression
	}{
		{
			searchExpr: search.NewSearch(expression.NewConstant(``),
				expression.NewConstant(map[string]interface{}{
					"match": "Salt Lake City",
					"field": "city",
				})),
		},
		{
			searchExpr: search.NewSearch(expression.NewConstant(``),
				expression.NewConstant(map[string]interface{}{
					"query": map[string]interface{}{
						"match": "Salt Lake City",
						"field": "city",
					},
				})),
		},
	}

	var expectQuery map[string]interface{}
	_ = json.Unmarshal([]byte(`{"query":{"conjuncts":[{"field":"country",`+
		`"term":"United States"},{"field":"city","match":"Salt Lake City",`+
		`"fuzziness": 0,"prefix_length": 0}]},"score":"none"}`),
		&expectQuery)

	for testi, test := range tests {
		finalExpr := expression.NewAnd(flexExpr, test.searchExpr)

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     finalExpr,
		}

		resp, n1qlErr := index.SargableFlex("0", flexRequest)
		if n1qlErr != nil {
			t.Fatal(n1qlErr)
		}

		expectedSargKeys := []string{test.searchExpr.String(), "country"}

		if resp == nil || len(resp.StaticSargKeys) != len(expectedSargKeys) {
			t.Fatalf("[%d] Resp: %#v", testi+1, resp)
		}

		for _, key := range expectedSargKeys {
			if resp.StaticSargKeys[key] == nil {
				t.Fatalf("[%d] ExpectedSargKeys: %v, Got StaticSargKeys: %v",
					testi+1, expectedSargKeys, resp.StaticSargKeys)
			}
		}

		var gotQuery map[string]interface{}
		if err := json.Unmarshal([]byte(resp.SearchQuery), &gotQuery); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expectQuery, gotQuery) {
			t.Fatalf("[%d] Expected query: %v, Got query: %v",
				testi+1, expectQuery, gotQuery)
		}
	}
}

func TestFlexPushDownSearchFunc2(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithSeveralNestedFieldsUnderHotelMapping)
	if err != nil {
		t.Fatal(err)
	}

	condExpr, _ := parser.Parse(`t.type = "hotel"`)
	flexExpr, _ := parser.Parse(
		`ANY p IN t.public_likes SATISFIES p LIKE "ABC" END`)

	tests := []struct {
		searchExpr       expression.Expression
		expectQuery      string
		expectedSargKeys []string
		expectSearchExpr bool
	}{
		{
			searchExpr: search.NewSearch(expression.NewConstant(``),
				expression.NewConstant(`reviews.author:XYZ`),
				expression.NewConstant(map[string]interface{}{
					"index": "SampleIndexDefWithSeveralNestedFieldsUnderHotelMapping",
				})),
			expectQuery: `{"query":{"conjuncts":[{"field":"public_likes",` +
				`"wildcard":"ABC"},{"must":{"conjuncts":[]},"must_not":{"disjuncts":[],` +
				`"min": 0},"should":{"disjuncts":[{"field":"reviews.author","fuzziness":0,` +
				`"match":"XYZ","prefix_length":0}],"min":0}}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "public_likes"},
			expectSearchExpr: true,
		},
		{
			searchExpr: search.NewSearch(expression.NewConstant(``),
				expression.NewConstant(`reviews.author:XYZ`),
				expression.NewConstant(map[string]interface{}{
					"index": "Incorrect Name",
				})),
			expectQuery: `{"query":{"field":"public_likes",` +
				`"wildcard":"ABC"},"score":"none"}`,
			expectedSargKeys: []string{"type", "public_likes"},
			expectSearchExpr: false,
		},
	}

	for testi, test := range tests {
		expr := expression.NewAnd(condExpr, flexExpr, test.searchExpr)

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     expr,
		}

		resp, n1qlErr := index.SargableFlex("0", flexRequest)
		if n1qlErr != nil {
			t.Fatal(n1qlErr)
		}

		if test.expectSearchExpr {
			test.expectedSargKeys = append(test.expectedSargKeys,
				test.searchExpr.String())
		}

		if resp == nil || len(resp.StaticSargKeys) != len(test.expectedSargKeys) {
			t.Fatalf("[%d] Resp: %#v", testi+1, resp)
		}

		for _, key := range test.expectedSargKeys {
			if resp.StaticSargKeys[key] == nil {
				t.Fatalf("ExpectedSargKeys: %v, Got StaticSargKeys: %v",
					test.expectedSargKeys, resp.StaticSargKeys)
			}
		}

		var expectQuery, gotQuery map[string]interface{}
		if err := json.Unmarshal([]byte(test.expectQuery), &expectQuery); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(resp.SearchQuery), &gotQuery); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expectQuery, gotQuery) {
			t.Fatalf("[%d] Expected query: %v, Got query: %v", testi+1, expectQuery, gotQuery)
		}
	}
}

func TestFlexPushDownSearchFunc3(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithMultipleTypeMappings)
	if err != nil {
		t.Fatal(err)
	}

	condExpr, _ := parser.Parse(`t.type = "airport"`)
	flexExpr, _ := parser.Parse(`t.country = "United States"`)
	searchExpr := search.NewSearch(expression.NewConstant(``),
		expression.NewConstant(`city:"Salt Lake City"`))

	finalExpr := expression.NewAnd(condExpr, flexExpr, searchExpr)
	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     finalExpr,
	}

	resp, n1qlErr := index.SargableFlex("0", flexRequest)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	// searchExpr not supported because FTS index has multiple
	// type mappings
	expectedSargKeys := []string{"type", "country"}

	if resp == nil || len(resp.StaticSargKeys) != len(expectedSargKeys) {
		t.Fatalf("Resp: %#v", resp)
	}

	var expectQuery, gotQuery map[string]interface{}
	_ = json.Unmarshal([]byte(`{"query":{"field":"country","term":"United States"},`+
		`"score":"none"}`),
		&expectQuery)

	if err := json.Unmarshal([]byte(resp.SearchQuery), &gotQuery); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectQuery, gotQuery) {
		t.Fatalf("Expected query: %v, Got query: %v", expectQuery, gotQuery)
	}
}

func TestIndexSargabilityDocIDRegexp(t *testing.T) {
	index, err := setupSampleIndex([]byte(`
		{
			"type": "fulltext-index",
			"name": "idx1",
			"sourceType": "gocbcore",
			"sourceName": "travel-sample",
			"planParams": {
				"indexPartitions": 6
			},
			"params": {
				"doc_config": {
					"mode": "docid_regexp",
					"docid_regexp": "abc.*ghi"
				},
				"mapping": {
					"default_mapping": {
						"enabled": false
					},
					"types": {
						"abcdefghi": {
							"enabled": true,
							"dynamic": false,
							"properties": {
								"country": {
									"enabled": true,
									"dynamic": false,
									"fields": [
									{
										"analyzer": "keyword",
										"index": true,
										"name": "country",
										"type": "text"
									}
									]
								}
							}
						}
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
		}
		`))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQueryStr string
	}{
		{
			queryStr:         `meta(t).id LIKE "%abcdefghi%" AND t.country = "US"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			queryStr:         `meta(t).id IN ["abc.*ghi"] AND t.country = "US"`,
			expectedQueryStr: ``,
		},
	}

	for i := range tests {
		queryExpression, err := parser.Parse(tests[i].queryStr)
		if err != nil {
			t.Fatal(tests[i].queryStr, err)
		}

		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExpression,
		}

		resp, err := index.SargableFlex("0", flexRequest)
		if err != nil {
			t.Fatal(err)
		}

		if len(tests[i].expectedQueryStr) == 0 {
			if resp != nil {
				t.Errorf("[%d] Expected `%s` to not be sargable for index",
					i, tests[i].queryStr)
			}
			continue
		}

		if resp == nil {
			t.Errorf("[%d] Expected `%s` to be sargable for index", i, tests[i].queryStr)
			continue
		}

		var gotQuery, expectedQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Errorf("[%d] SearchQuery: %s, err: %v", i, resp.SearchQuery, err)
		}
		err = json.Unmarshal([]byte(tests[i].expectedQueryStr), &expectedQuery)
		if err != nil {
			t.Errorf("[%d] ExpectedQuery: %s, err: %v", i, tests[i].expectedQueryStr, err)
		}

		if !reflect.DeepEqual(expectedQuery, gotQuery) {
			t.Errorf("[%d] ExpectedQuery: %s, GotQuery: %s",
				i, tests[i].expectedQueryStr, resp.SearchQuery)
		}
	}
}

func TestIndexSargabilityForQueriesThatNeedFiltering(t *testing.T) {
	index, err := setupSampleIndex([]byte(`{
		"name": "hotels",
		"type": "fulltext-index",
		"sourceName": "travel-sample",
		"planParams": {
			"indexPartitions": 6
		},
		"params": {
			"doc_config": {
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"default_mapping": {
					"dynamic": true,
					"enabled": false
				},
				"type_field": "_type",
				"types": {
					"hotel": {
						"dynamic": true,
						"enabled": true
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

	queBytes := []byte(`
	{
		"conjuncts":[{
			"match": "United States",
			"field": "country",
			"analyzer": "keyword"
		}, {
			"match": "San Francisco",
			"field": "city"
		}]
	}
	`)
	var que map[string]interface{}
	err = json.Unmarshal(queBytes, &que)
	if err != nil {
		t.Fatal(err)
	}

	// MB-47538
	q1Bytes := []byte(`{"query": {"match_all": {}}}`)
	var q1 map[string]interface{}
	err = json.Unmarshal(q1Bytes, &q1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, exact, _, n1qlErr := index.Sargable("",
		expression.NewConstant(q1), expression.NewConstant(``), nil)
	if n1qlErr != nil || exact {
		t.Errorf("Expected exact: false for query: %v, err: %v", q1, n1qlErr)
	}

	// MB-47276
	q2 := `-country:France`
	_, _, exact, _, n1qlErr = index.Sargable("",
		expression.NewConstant(q2), expression.NewConstant(``), nil)
	if n1qlErr != nil || exact {
		t.Errorf("Expected exact: false for query: %v, err: %v", q2, n1qlErr)
	}

	q3Bytes := []byte(`{"must_not": {"disjuncts": [{"match": "France", "field": "country"}]}}`)
	var q3 map[string]interface{}
	err = json.Unmarshal(q3Bytes, &q3)
	if err != nil {
		t.Fatal(err)
	}
	_, _, exact, _, n1qlErr = index.Sargable("",
		expression.NewConstant(q3), expression.NewConstant(``), nil)
	if n1qlErr != nil || exact {
		t.Errorf("Expected exact: false for query: %v, err: %v", q3, n1qlErr)
	}
}

func TestMB51888(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicWithAnalyzerKeyword)
	if err != nil {
		t.Fatal(err)
	}

	queryStr := `ANY c IN t.children SATISFIES c.gender = "F"` +
		` AND (c.age > 5 AND c.age <15) OR c.first_name LIKE "a%" END`

	queryExpression, err := parser.Parse(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     queryExpression,
	}

	resp, err := index.SargableFlex("0", flexRequest)
	if err != nil {
		t.Fatal(err)
	}

	if resp.RespFlags&datastore.FTS_FLEXINDEX_EXACT == 1 {
		t.Fatalf("expected response to not be exact")
	}
}

func TestMB52163(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefDynamicWithAnalyzerKeyword)
	if err != nil {
		t.Fatal(err)
	}

	queryStr := `ANY l in t.arr SATISFIES l IN ["1", "2", "3"] END`
	queryExpression, err := parser.Parse(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	flexRequest := &datastore.FTSFlexRequest{
		Keyspace: "t",
		Pred:     queryExpression,
	}

	resp, err := index.SargableFlex("0", flexRequest)
	if err != nil {
		t.Fatal(err)
	}

	if resp == nil {
		t.Fatalf("Expected query to be sargable for index")
	}

	expectedQueryStr := `
	{
		"query": {
			"disjuncts": [
			{
				"field": "arr",
				"term": "1"
			},
			{
				"field": "arr",
				"term": "2"
			},
			{
				"field": "arr",
				"term": "3"
			}
			]
		},
		"score": "none"
	}
	`
	var gotQuery, expectedQuery map[string]interface{}
	err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
	if err != nil {
		t.Errorf("SearchQuery: %s, err: %v", resp.SearchQuery, err)
	}
	err = json.Unmarshal([]byte(expectedQueryStr), &expectedQuery)
	if err != nil {
		t.Errorf("ExpectedQuery: %s, err: %v", expectedQueryStr, err)
	}

	if !reflect.DeepEqual(expectedQuery, gotQuery) {
		t.Errorf("ExpectedQuery: %s, GotQuery: %s",
			expectedQueryStr, resp.SearchQuery)
	}
}

// MB-52465
func TestFlexNeedForFiltering(t *testing.T) {
	tests := []struct {
		indexDef []byte
		queryStr string
	}{
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "travel-sample",
				"params": {
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"xyz": {
								"dynamic": true,
								"enabled": true,
								"default_analyzer": "keyword"
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}
			}`),
			queryStr: `t.type = "xyz" AND t.country = "US"`,
		},
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "travel-sample",
				"params": {
					"doc_config": {
						"mode": "docid_prefix",
						"docid_prefix_delim": "-"
					},
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"xyz": {
								"dynamic": true,
								"enabled": true,
								"default_analyzer": "keyword"
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}
			}`),
			queryStr: `meta(t).id LIKE "xyz-%" AND t.country = "US"`,
		},
		{
			indexDef: []byte(`{
				"name": "temp",
				"type": "fulltext-index",
				"sourceName": "travel-sample",
				"params": {
					"doc_config": {
						"mode": "docid_regexp",
						"docid_regexp": ".*xyz.*"
					},
					"mapping": {
						"default_mapping": {
							"enabled": false
						},
						"types": {
							"xyz": {
								"dynamic": true,
								"enabled": true,
								"default_analyzer": "keyword"
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}
			}`),
			queryStr: `meta(t).id LIKE "%xyz%" AND t.country = "US"`,
		},
	}

	for i := range tests {
		index, err := setupSampleIndex(tests[i].indexDef)
		if err != nil {
			t.Fatal(err)
		}

		queryExpr, err := parser.Parse(tests[i].queryStr)
		if err != nil {
			t.Fatal(tests[i].queryStr, err)
		}
		flexRequest := &datastore.FTSFlexRequest{
			Keyspace: "t",
			Pred:     queryExpr,
		}

		resp, n1qlErr := index.SargableFlex("0", flexRequest)
		if n1qlErr != nil {
			t.Fatal(n1qlErr)
		}

		if resp == nil {
			t.Fatalf("[%d] Expected a valid response", i+1)
		}

		if resp.RespFlags&datastore.FTS_FLEXINDEX_EXACT == 0 {
			t.Errorf("[%d] Expecting an exact result, shouldn't be a need for filtering", i+1)
		}
	}
}
