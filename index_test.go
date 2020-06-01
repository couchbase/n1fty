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

	if pip.SearchFields != nil || len(pip.DynamicMappings) > 0 {
		return newFTSIndex(nil, indexDef, pip)
	}

	return nil, fmt.Errorf("failed to setup index")
}

// =============================================================================

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
					`"inclusive_max":true,"inclusive_min":true,"max":4,"min":4}]}]},"score":"none"}`,
				expectedQueryStrCustomMapping: `{"query":{"disjuncts":[{"field":"reviews.ratings.Cleanliness",` +
					`"inclusive_max":true,"inclusive_min":true,"max":5,"min":5},` +
					`{"field":"reviews.ratings.Overall","inclusive_max":true,"inclusive_min":true,` +
					`"max":4,"min":4}]},"score":"none"}`,
				expectedSargKeys: []string{"type", "reviews.ratings.Cleanliness", "reviews.ratings.Overall"},
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
			// MB-39517: "city" not indexed but country is, so partially sargable.
			queryStr:         `(t.type = "airline" OR t.type = "airport") AND t.country = "US" AND t.city = "SF"`,
			expectedQueryStr: `{"query":{"field":"country","term":"US"},"score":"none"}`,
		},
		{
			// MB-39517: "city" not indexed and although country is, it's a disjunction
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
