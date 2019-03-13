// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/query/value"
)

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		field string
		query value.Value
	}{
		{
			field: "title",
			query: value.NewValue(`+Avengers~2 company:marvel`),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match":     "avengers",
				"field":     "title",
				"fuzziness": 2,
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match_phrase": "Avengers: Infinity War",
				"field":        "title",
				"analyzer":     "en",
				"boost":        10,
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"wildcard": "Avengers*",
				"field":    "title",
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"conjuncts": []interface{}{
					map[string]interface{}{
						"match": "abc",
						"field": "cba",
					},
					map[string]interface{}{
						"match": "xyz",
						"field": "zyx",
					}},
			}),
		},
	}

	for i, test := range tests {
		q, err := BuildQuery(test.field, test.query)
		if err != nil {
			t.Fatal(err)
		}

		switch qq := q.(type) {
		case *query.BooleanQuery:
			cq := qq.Must.(*query.ConjunctionQuery)
			if len(cq.Conjuncts) != 1 {
				t.Fatalf("Exception in boolean query, number of must clauses: %v",
					len(cq.Conjuncts))
			}
			mcq := cq.Conjuncts[0].(*query.MatchQuery)
			if mcq.Match != "Avengers" || mcq.FieldVal != "title" || mcq.Fuzziness != 2 {
				t.Fatalf("Exception in boolean must query: %v, %v, %v",
					mcq.Match, mcq.FieldVal, mcq.Fuzziness)
			}
			dq := qq.Should.(*query.DisjunctionQuery)
			if len(dq.Disjuncts) != 1 {
				t.Fatalf("Exception in boolean query, number of should clauses: %v",
					len(dq.Disjuncts))
			}
			mdq := dq.Disjuncts[0].(*query.MatchQuery)
			if mdq.Match != "marvel" || mdq.FieldVal != "company" {
				t.Fatalf("Exception in boolean should query: %v, %v",
					mdq.Match, mdq.FieldVal)
			}
		case *query.MatchQuery:
			if qq.Match != "avengers" || qq.FieldVal != "title" || qq.Fuzziness != 2 {
				t.Fatalf("Exception in match query: %v, %v, %v",
					qq.Match, qq.FieldVal, qq.Fuzziness)
			}
		case *query.MatchPhraseQuery:
			if qq.MatchPhrase != "Avengers: Infinity War" || qq.FieldVal != "title" ||
				qq.Analyzer != "en" || float64(*qq.BoostVal) != float64(10) {
				t.Fatalf("Exception in match phrase query: %v, %v, %v, %v",
					qq.MatchPhrase, qq.FieldVal, qq.Analyzer, *qq.BoostVal)
			}
		case *query.WildcardQuery:
			if qq.Wildcard != "Avengers*" || qq.FieldVal != "title" {
				t.Fatalf("Exception in wildcard query: %v, %v", qq.Wildcard, qq.FieldVal)
			}
		case *query.ConjunctionQuery:
			if len(qq.Conjuncts) != 2 {
				t.Fatalf("Exception in conjunction query: %v", len(qq.Conjuncts))
			}
			mq1, ok1 := qq.Conjuncts[0].(*query.MatchQuery)
			mq2, ok2 := qq.Conjuncts[1].(*query.MatchQuery)
			if !ok1 || !ok2 || mq1.Field() != "cba" || mq2.Field() != "zyx" {
				t.Fatalf("Exception in conjunction query")
			}
		default:
			t.Fatalf("Unexpected query type: %v, for entry: %v", reflect.TypeOf(q), i)
		}
	}
}

func TestBuildBadQuery(t *testing.T) {
	q := value.NewValue(map[string]interface{}{
		"this": "is",
		"a":    "very",
		"bad":  "example",
	})

	if _, err := BuildQuery("", q); err == nil {
		t.Fatal("Expected an error, but didn't see one")
	}
}

func TestBuildSearchRequest(t *testing.T) {
	tests := []struct {
		id    int
		query value.Value
	}{
		{
			id: 1,
			query: value.NewValue(map[string]interface{}{
				"from": 90,
				"size": 100,
				"query": value.NewValue(map[string]interface{}{
					"wildcard": "Avengers*",
					"field":    "title",
				}),
				"explain":          true,
				"includeLocations": true,
				"fields":           []interface{}{"country", "city"},
				"Sort": value.NewValue(map[string]interface{}{
					"by":    "geo_distance",
					"field": "geo",
					"unit":  "mi",
					"location": map[string]interface{}{
						"lon": -2.235143,
						"lat": 53.482358,
					},
				}),
			}),
		},
		{
			id: 2,
			query: value.NewValue(map[string]interface{}{
				"from": 100,
				"size": 10,
				"query": value.NewValue(map[string]interface{}{
					"match":     "avengers",
					"field":     "title",
					"fuzziness": 2,
				}),
			}),
		},
		{
			id: 3,
			query: value.NewValue(map[string]interface{}{
				"from": 1000,
				"size": 10,
				"query": value.NewValue(map[string]interface{}{
					"match_phrase": "Avengers: Infinity War",
					"field":        "title",
					"analyzer":     "en",
					"boost":        10,
				}),
				"includeLocations": true,
				"fields":           []interface{}{"country", "city"},
			}),
		},
		{
			id: 4,
			query: value.NewValue(map[string]interface{}{
				"from": 1000,
				"size": 10,
				"query": value.NewValue(map[string]interface{}{
					"conjuncts": []interface{}{
						map[string]interface{}{
							"match": "abc",
							"field": "cba",
						},
						map[string]interface{}{
							"match": "xyz",
							"field": "zyx",
						}},
				}),
				"Sort": []interface{}{"country", "_id", "-_score"},
			}),
		},
	}

	for i, test := range tests {
		sr, q, err := BuildSearchRequest("", test.query)
		if err != nil {
			t.Fatalf("Expected no error, but got err: %v", err)
		}

		switch qq := q.(type) {
		case *query.MatchQuery:
			if qq.Match != "avengers" || qq.FieldVal != "title" || qq.Fuzziness != 2 {
				t.Fatalf("Exception in match query: %v, %v, %v",
					qq.Match, qq.FieldVal, qq.Fuzziness)
			}

			if sr.Size != 10 || sr.From != 100 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

		case *query.WildcardQuery:
			if qq.Wildcard != "Avengers*" || qq.FieldVal != "title" {
				t.Fatalf("Exception in wildcard query: %v, %v", qq.Wildcard, qq.FieldVal)
			}

			if sr.Size != 100 || sr.From != 90 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

		case *query.MatchPhraseQuery:
			if qq.MatchPhrase != "Avengers: Infinity War" || qq.FieldVal != "title" ||
				qq.Analyzer != "en" || float64(*qq.BoostVal) != float64(10) {
				t.Fatalf("Exception in match phrase query: %v, %v, %v, %v",
					qq.MatchPhrase, qq.FieldVal, qq.Analyzer, *qq.BoostVal)
			}

			if sr.Size != 10 || sr.From != 1000 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

			if !reflect.DeepEqual(sr.Fields, []string{"country", "city"}) {
				t.Fatalf("incorrect search request, fields: %v", sr.Fields)
			}

		case *query.ConjunctionQuery:
			if len(qq.Conjuncts) != 2 {
				t.Fatalf("Exception in conjunction query: %v", len(qq.Conjuncts))
			}
			mq1, ok1 := qq.Conjuncts[0].(*query.MatchQuery)
			mq2, ok2 := qq.Conjuncts[1].(*query.MatchQuery)
			if !ok1 || !ok2 || mq1.Field() != "cba" || mq2.Field() != "zyx" {
				t.Fatalf("Exception in conjunction query")
			}

			sbytes, _ := json.Marshal([]string{"country", "_id", "-_score"})
			if !reflect.DeepEqual(sbytes, sr.Sort) {
				t.Fatalf("incorrect search request, Sort: %v", sr.Sort)
			}

		default:
			t.Fatalf("Unexpected query type: %v, for entry: %v", reflect.TypeOf(q), i)
		}
	}
}
