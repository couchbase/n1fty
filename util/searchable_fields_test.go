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
	"github.com/couchbase/query/value"
)

func TestIndexDefConversion(t *testing.T) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(SampleCustomIndexDef, &indexDef)
	if err != nil {
		t.Fatal(err)
	}

	searchFieldsMap, dynamicMapping, defaultAnalyzer :=
		SearchableFieldsForIndexDef(indexDef)
	if searchFieldsMap == nil || dynamicMapping || defaultAnalyzer != "standard" {
		t.Fatalf("unexpected return values from SearchFieldsForIndexDef")
	}

	expect := map[SearchField]bool{}
	expect[SearchField{Name: "reviews.review", Analyzer: "standard"}] = true
	expect[SearchField{Name: "country", Analyzer: "da", Type: "text"}] = false
	expect[SearchField{Name: "countryX", Analyzer: "standard", Type: "text"}] = false
	expect[SearchField{Name: "reviews.id", Analyzer: "standard", Type: "text"}] = false

	if !reflect.DeepEqual(expect, searchFieldsMap) {
		t.Fatalf("Expected: %v, Got: %v", expect, searchFieldsMap)
	}
}

func TestFieldsToSearch(t *testing.T) {
	tests := []struct {
		field  string
		query  value.Value
		expect []string
	}{
		{
			field:  "title",
			query:  value.NewValue(`+Avengers~2 company:marvel`),
			expect: []string{"company", "title"},
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match":     "avengers",
				"field":     "title",
				"fuzziness": 2,
			}),
			expect: []string{"title"},
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match_phrase": "Avengers: Infinity War",
				"field":        "title",
				"analyzer":     "en",
				"boost":        10,
			}),
			expect: []string{"title"},
		},
		{
			field: "title",
			query: value.NewValue(map[string]interface{}{
				"wildcard": "Avengers*",
				"field":    "title",
			}),
			expect: []string{"title"},
		},
		{
			field:  "title",
			query:  value.NewValue(`+movie:Avengers +sequel.id:3 +company:marvel`),
			expect: []string{"company", "movie", "sequel.id", "sequel.id"},
			// Expect 2 sequel.id entries above as the number look up above is
			// considered as a disjunction of a match and a numeric range.
		},
	}

	for _, test := range tests {
		qBytes, err := BuildQueryBytes(test.field, test.query)
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
