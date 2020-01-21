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

func setupSampleIndex(idef []byte) (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(idef, &indexDef)
	if err != nil {
		return nil, err
	}

	pip, err := util.ProcessIndexDef(indexDef)
	if err != nil {
		return nil, err
	}

	if pip.SearchFields != nil || pip.Dynamic {
		return newFTSIndex(nil, indexDef, pip.IndexMapping,
			pip.SearchFields, pip.IndexedCount, pip.CondExpr,
			pip.Dynamic, pip.AllFieldSearchable,
			pip.DefaultAnalyzer, pip.DefaultDateTimeParser)
	}

	return nil, fmt.Errorf("failed to setup index")
}

func TestTypeFieldWithSpecialCharacterIndexSargability(t *testing.T) {
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

func TestDynamicDefaultIndexNoFieldsQuerySargability(t *testing.T) {
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

func TestCustomIndexNoFieldsQuerySargability(t *testing.T) {
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

	if count != 1 || indexedCount != 3 || !exact {
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

func TestCompatibleCustomDefaultMappedIndexSargability(t *testing.T) {
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

func TestDateTimeSargability(t *testing.T) {
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

func TestInvalidIndexNameSargability(t *testing.T) {
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

func TestCustomIndexNoAllFiedlSargability(t *testing.T) {
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

func TestSargableFlexIndex(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		queryStr         string
		expectedQuery    string
		expectedSargKeys []string
	}{
		{
			queryStr:         "t.type = 'hotel' AND t.country = 'United States'",
			expectedQuery:    `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.type = 'hotel' AND t.id = 10",
			expectedQuery: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
				`{"field":"id","inclusive_max":true,"inclusive_min":true,"max":10,` +
				`"min":10}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "id"},
		},
		{
			queryStr: "t.type = 'hotel' AND (t.id = 10 OR t.id = 20)",
			expectedQuery: `{"query":{"conjuncts":[{"field":"type","term":"hotel"},` +
				`{"disjuncts":[{"field":"id","inclusive_max":true,"inclusive_min":true,` +
				`"max":10,"min":10},{"field":"id","inclusive_max":true,` +
				`"inclusive_min":true,"max":20,"min":20}]}]},"score":"none"}`,
			expectedSargKeys: []string{"type", "id"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id < 10",
			expectedQuery:    `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id <= 10",
			expectedQuery:    `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id > 10",
			expectedQuery:    `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr:         "t.type = 'hotel' AND t.id >= 10",
			expectedQuery:    `{"query":{"field":"type","term":"hotel"},"score":"none"}`,
			expectedSargKeys: []string{"type"},
		},
		{
			queryStr: "t.isOpen = true AND t.type = 'hotel'",
			expectedQuery: `{"query":{"conjuncts":[{"field":"isOpen","bool":true},` +
				`{"field":"type", "term": "hotel"}]}, "score": "none"}`,
			expectedSargKeys: []string{"isOpen", "type"},
		},
	}

	for testi, test := range tests {
		queryExp, err := parser.Parse(test.queryStr)
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

		if resp == nil || len(resp.StaticSargKeys) != len(test.expectedSargKeys) {
			t.Fatalf("[test: %v] Query: %v, Resp: %#v", testi+1, test.queryStr, resp)
		}

		for _, key := range test.expectedSargKeys {
			if resp.StaticSargKeys[key] == nil {
				t.Fatalf("[test: %v] ExpectedSargKeys: %v, Got StaticSargKeys: %v",
					testi+1, test.expectedSargKeys, resp.StaticSargKeys)
			}
		}

		var gotQuery, expectedQuery map[string]interface{}
		err = json.Unmarshal([]byte(resp.SearchQuery), &gotQuery)
		if err != nil {
			t.Fatalf("[test: %v] SearchQuery: %s, err: %v", testi+1, resp.SearchQuery, err)
		}
		err = json.Unmarshal([]byte(test.expectedQuery), &expectedQuery)
		if err != nil {
			t.Fatalf("[test: %v] ExpectedQuery: %s, err: %v", testi+1, test.expectedQuery, err)
		}

		if !reflect.DeepEqual(expectedQuery, gotQuery) {
			t.Fatalf("[test: %v] ExpectedQuery: %#v, GotQuery: %#v",
				testi+1, expectedQuery, gotQuery)
		}
	}
}

func TestNotSargableFlexIndex(t *testing.T) {
	index, err := setupSampleIndex(
		util.SampleIndexDefWithKeywordAnalyzerOverDefaultMapping)
	if err != nil {
		t.Fatal(err)
	}

	queryStr := "t.country = 'United States'"

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
		t.Fatalf("Expected query to be NOT-SARGABLE")
	}
}

func TestSargableDynamicFlexIndex(t *testing.T) {
	// Only a default dynamic index with "default_analzyer" set to "keyword"
	// and "default_datetime_parser" set to "disabled" is flex-sargable.
	index, err := setupSampleIndex(
		util.SampleIndexDefDynamicWithAnalyzerKeywordDateTimeDisabled)
	if err != nil {
		t.Fatal(err)
	}

	queryStr := "t.country = 'United States'"

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

	if resp == nil || len(resp.StaticSargKeys) != 1 ||
		resp.StaticSargKeys["country"] == nil {
		t.Fatalf("Resp: %#v", resp)
	}

	expectedQueryStr := `{"query":{"field":"country","term":"United States"},"score":"none"}`

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
		t.Fatalf("ExpectedQuery: %#v, GotQuery: %#v", expectedQuery, gotQuery)
	}
}
