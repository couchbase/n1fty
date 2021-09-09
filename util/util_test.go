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

	"github.com/couchbase/query/value"
)

func TestBuildIndexMappingOnFields(t *testing.T) {
	fields := map[SearchField]struct{}{}
	for _, i := range []SearchField{
		{
			Name: "reviews.review.author",
			Type: "text",
		},
		{
			Name: "reviews.review.age",
			Type: "number",
		},
		{
			Name: "reviews.review.dob",
			Type: "datetime",
		},
		{
			Name:     "startDate",
			Type:     "text",
			Analyzer: "keyword",
		},
		{
			Name: "details.title",
			Type: "text",
		},
	} {
		fields[i] = struct{}{}
	}

	expectBytes := []byte(`
	{
		"default_mapping": {
			"enabled": true,
			"dynamic": false,
			"properties": {
				"details": {
					"enabled": true,
					"dynamic": false,
					"properties": {
						"title": {
							"enabled": true,
							"dynamic": false,
							"fields": [{
								"name": "title",
								"type": "text",
								"index": true,
								"include_term_vectors": true
							}]
						}
					}
				},
				"reviews": {
					"enabled": true,
					"dynamic": false,
					"properties": {
						"review": {
							"enabled": true,
							"dynamic": false,
							"properties": {
								"dob": {
									"enabled": true,
									"dynamic": false,
									"fields": [{
										"name": "dob",
										"type": "datetime",
										"index": true,
										"include_term_vectors": true
									}]
								},
								"age": {
									"enabled": true,
									"dynamic": false,
									"fields": [{
										"name": "age",
										"type": "number",
										"index": true,
										"include_term_vectors": true
									}]
								},
								"author": {
									"enabled": true,
									"dynamic": false,
									"fields": [{
										"name": "author",
										"type": "text",
										"index": true,
										"include_term_vectors": true
									}]
								}
							}
						}
					}
				},
				"startDate": {
					"enabled": true,
					"dynamic": false,
					"fields": [{
						"name": "startDate",
						"type": "text",
						"analyzer": "keyword",
						"index": true,
						"include_term_vectors": true
					}]
				}
			}
		},
		"type_field": "_type",
		"default_type": "_default",
		"default_analyzer": "standard",
		"default_datetime_parser": "dateTimeOptional",
		"default_field": "_all",
		"store_dynamic": true,
		"index_dynamic": true,
		"docvalues_dynamic": true,
		"analysis": {}
	}
	`)

	idxMapping := BuildIndexMappingOnFields(fields, "", "")
	gotBytes, err := json.Marshal(idxMapping)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]interface{}
	err = json.Unmarshal(gotBytes, &got)
	if err != nil {
		t.Fatal(err)
	}

	var expect map[string]interface{}
	err = json.Unmarshal(expectBytes, &expect)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expect, got) {
		t.Fatalf("Expected:\n\t%v\nGot:\n\t%v", string(expectBytes), string(gotBytes))
	}
}

// https://issues.couchbase.com/browse/MB-44356
func TestParseQueryToSearchRequest(t *testing.T) {
	expectQueryFields := map[SearchField]struct{}{
		{Name: "app_name", Type: "text"}: struct{}{},
	}

	tests := []interface{}{
		map[string]interface{}{"match": "dark", "field": "app_name"},
		map[string]interface{}{"query": map[string]interface{}{"match": "dark", "field": "app_name"}},
		`app_name:dark`,
		map[string]interface{}{"query": "app_name:dark"},
		map[string]interface{}{"query": map[string]interface{}{"query": "app_name:dark"}},
	}

	for _, test := range tests {
		q := value.NewValue(test)
		gotQueryFields, _, _, _, err := ParseQueryToSearchRequest("", q)
		if err != nil {
			t.Fatal(test, err)
		}

		if !reflect.DeepEqual(expectQueryFields, gotQueryFields) {
			t.Fatal(test, gotQueryFields)
		}
	}
}
