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
