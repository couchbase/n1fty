//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package util

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/couchbase/cbgt"
)

func TestIndexDefConversion(t *testing.T) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(SampleIndexDef, &indexDef)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := SearchableFieldsForIndexDef(indexDef)
	if got == nil {
		t.Fatalf("expected a set of searchable fields")
	}

	gotMap := map[string][]string{}
	for k, v := range got {
		gotMap[k] = []string{}
		for _, entry := range v {
			gotMap[k] = append(gotMap[k], entry.Name)
		}
		sort.Strings(gotMap[k])
	}

	expect := map[string][]string{}
	expect["landmark"] = []string{"countryX", "reviews.id", "reviews.review"}
	expect["hotel"] = []string{"country"}

	if !reflect.DeepEqual(expect, gotMap) {
		t.Fatalf("Expected: %v, Got: %v", expect, gotMap)
	}
}

func TestFieldsToSearch(t *testing.T) {
	tests := []struct {
		field   string
		query   string
		options []byte
		expect  []string
	}{
		{
			field:   "title",
			query:   "+Avengers~2 company:marvel",
			options: []byte(""),
			expect:  []string{"company", "title"},
		},
		{
			field:   "title",
			query:   "avengers",
			options: []byte(`{"type": "match", "fuzziness": 2}`),
			expect:  []string{"title"},
		},
		{
			field:   "title",
			query:   "Avengers: Infinity War",
			options: []byte(`{"type": "match_phrase", "analyzer": "en", "boost": 10}`),
			expect:  []string{"title"},
		},
		{
			field:   "title",
			query:   "Avengers*",
			options: []byte(`{"type": "wildcard"}`),
			expect:  []string{"title"},
		},
		{
			field:   "title",
			query:   "+movie:Avengers +sequel.id:3 +company:marvel",
			options: []byte(``),
			expect:  []string{"company", "movie", "sequel.id", "sequel.id"},
			// Expect 2 sequel.id entries above as the number look up above is
			// considered as a disjunction of a match and a numeric range.
		},
	}

	for _, test := range tests {
		qBytes, err := BuildQueryBytes(test.field, test.query, test.options)
		if err != nil {
			t.Fatal(err)
		}
		fieldDescs, err := FetchFieldsToSearchFromQuery(qBytes)
		if err != nil {
			t.Fatal(err)
		}

		fields := []string{}
		for _, entry := range fieldDescs {
			fields = append(fields, entry.Name)
		}

		sort.Strings(fields)
		if !reflect.DeepEqual(test.expect, fields) {
			t.Fatalf("Expected: %v, Got: %v", test.expect, fields)
		}
	}
}
