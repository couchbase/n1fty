package n1fty

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
)

func setupSampleIndex(idef []byte) (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(idef, &indexDef)
	if err != nil {
		return nil, err
	}

	_, _, searchFields, condExpr, dynamic, defaultAnalyzer, err :=
		util.ProcessIndexDef(indexDef)
	if err != nil {
		return nil, err
	}

	if searchFields != nil || dynamic {
		return newFTSIndex(nil, indexDef,
			searchFields, condExpr, dynamic, defaultAnalyzer)
	}

	return nil, fmt.Errorf("failed to setup index")
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

	if indexedCount != 4 {
		t.Fatalf("Expected indexed count of 4, but got: %v", indexedCount)
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

	if indexedCount != 4 {
		t.Fatalf("Expected indexed count of 4, but got: %v", indexedCount)
	}
}

func TestDynamicIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithAnalyzerEN)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match":    "United States",
		"field":    "country",
		"analyzer": "default",
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
			"field": "country"
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
							"name": "country", "type": "text", "index": true
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

	if indexedCount != 2 {
		t.Fatalf("Expected indexed count of 2, but got: %v", indexedCount)
	}
}

func TestIndexPageable(t *testing.T) {
	index, err := setupSampleIndex(util.SampleLandmarkIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	// non matching sort fields in order and in search request
	query := expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    10,
		"from":    0,
		"explain": false,
	})

	pageable := index.Pageable([]string{"score DESC"}, 0, 10, query,
		expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

	// matching sort fields in order and in search request
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    10,
		"from":    0,
		"explain": false,
		"sort":    []interface{}{"country", "city", "-_score"},
	})

	expOrder := []string{"country", "city", "-_score"}
	order := []string{"country", "city", "-_score"}

	pageable = index.Pageable(order, 0,
		10, query, expression.NewConstant(``))

	if !pageable {
		t.Fatalf("Expected to be pageable, but got: %v", pageable)
	}

	if !reflect.DeepEqual(order, expOrder) {
		t.Fatalf("order got changed, expected: %v, but got: %v", expOrder, order)
	}

	// non matching sort fields in order and in search request
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    10,
		"from":    0,
		"explain": false,
		"sort":    []interface{}{"country", "_id", "-_score"},
	})

	pageable = index.Pageable([]string{"country", "city", "-_score"}, 0,
		10, query, expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}

	// matching sort fields in order and in search request,
	// but in different order
	query = expression.NewConstant(map[string]interface{}{
		"query": map[string]interface{}{
			"match": "united",
			"field": "countryX",
		},
		"size":    10,
		"from":    0,
		"explain": false,
		"sort":    []interface{}{"country", "_id", "-_score"},
	})

	pageable = index.Pageable([]string{"_id", "-_score", "country"}, 0,
		10, query, expression.NewConstant(``))

	if pageable {
		t.Fatalf("Expected to be non pageable, but got: %v", pageable)
	}
}
