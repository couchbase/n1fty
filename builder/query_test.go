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

package builder

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/search/query"
)

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		field   string
		query   string
		options string
	}{
		{
			field:   "title",
			query:   "+Avengers~2 company:marvel",
			options: "",
		},
		{
			field:   "title",
			query:   "avengers",
			options: `{"type": "match", "fuzziness": 2}`,
		},
		{
			field:   "title",
			query:   "Avengers: Infinity War",
			options: `{"type": "match_phrase", "analyzer": "en", "boost": 10}`,
		},
		{
			field:   "title",
			query:   "Avengers*",
			options: `{"type": "wildcard"}`,
		},
	}

	for i, test := range tests {
		q, err := BuildQuery(test.field, test.query, test.options)
		if err != nil {
			t.Fatal(err)
		}

		switch q.(type) {
		case *query.BooleanQuery:
			bq := q.(*query.BooleanQuery)
			cq := bq.Must.(*query.ConjunctionQuery)
			if len(cq.Conjuncts) != 1 {
				t.Fatalf("Exception in boolean query, number of must clauses: %v",
					len(cq.Conjuncts))
			}
			mcq := cq.Conjuncts[0].(*query.MatchQuery)
			if mcq.Match != "Avengers" || mcq.FieldVal != "title" || mcq.Fuzziness != 2 {
				t.Fatalf("Exception in boolean must query: %v, %v, %v",
					mcq.Match, mcq.FieldVal, mcq.Fuzziness)
			}
			dq := bq.Should.(*query.DisjunctionQuery)
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
			mq := q.(*query.MatchQuery)
			if mq.Match != "avengers" || mq.FieldVal != "title" || mq.Fuzziness != 2 {
				t.Fatalf("Exception in match query: %v, %v, %v",
					mq.Match, mq.FieldVal, mq.Fuzziness)
			}
		case *query.MatchPhraseQuery:
			mpq := q.(*query.MatchPhraseQuery)
			if mpq.MatchPhrase != "Avengers: Infinity War" || mpq.FieldVal != "title" ||
				mpq.Analyzer != "en" || float64(*mpq.BoostVal) != float64(10) {
				t.Fatalf("Exception in match phrase query: %v, %v, %v, %v",
					mpq.MatchPhrase, mpq.FieldVal, mpq.Analyzer, *mpq.BoostVal)
			}
		case *query.WildcardQuery:
			wq := q.(*query.WildcardQuery)
			if wq.Wildcard != "Avengers*" || wq.FieldVal != "title" {
				t.Fatalf("Exception in wildcard query: %v, %v", wq.Wildcard, wq.FieldVal)
			}
		default:
			t.Fatalf("Unexpected query type: %v, for entry: %v", reflect.TypeOf(q), i)
		}
	}
}
