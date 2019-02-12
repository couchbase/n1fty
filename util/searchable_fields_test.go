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
	sampleIndexDef := []byte(`{
		"type": "fulltext-index",
		"name": "travel",
		"uuid": "xyz",
		"sourceType": "couchbase",
		"sourceName": "travel-sample",
		"sourceUUID": "",
		"planParams": {
			"maxPartitionsPerPIndex": 171
		},
		"params": {
			"doc_config": {
				"docid_prefix_delim": "",
				"docid_regexp": "",
				"mode": "type_field",
				"type_field": "type"
			},
			"mapping": {
				"analysis": {},
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"default_mapping": {
					"dynamic": true,
					"enabled": true
				},
				"default_type": "_default",
				"docvalues_dynamic": true,
				"index_dynamic": true,
				"store_dynamic": false,
				"type_field": "_type",
				"types": {
					"hotel": {
						"dynamic": false,
						"enabled": true,
						"properties": {
							"country": {
								"dynamic": false,
								"enabled": true,
								"fields": [{
									"docvalues": true,
									"include_in_all": true,
									"include_term_vectors": true,
									"index": true,
									"name": "country",
									"store": true,
									"type": "text"
								}]
							},
							"reviews": {
								"dynamic": false,
								"enabled": true,
								"properties": {
									"author": {
										"dynamic": false,
										"enabled": true,
										"fields": [{
											"docvalues": true,
											"include_in_all": true,
											"include_term_vectors": true,
											"index": true,
											"name": "author",
											"store": true,
											"type": "text"
										}]
									},
									"content": {
										"dynamic": false,
										"enabled": true,
										"fields": [{
											"docvalues": true,
											"include_in_all": true,
											"include_term_vectors": true,
											"index": true,
											"name": "content",
											"store": true,
											"type": "text"
										}]
									}
								}
							},
							"state": {
								"dynamic": false,
								"enabled": true,
								"fields": [{
									"docvalues": true,
									"include_in_all": true,
									"include_term_vectors": true,
									"index": true,
									"name": "state",
									"store": true,
									"type": "text"
								}]
							}
						}
					},
					"landmark": {
						"dynamic": false,
						"enabled": true,
						"properties": {
							"country": {
								"dynamic": false,
								"enabled": true,
								"fields": [{
									"docvalues": true,
									"include_in_all": true,
									"include_term_vectors": true,
									"index": true,
									"name": "country",
									"store": true,
									"type": "text"
								}]
							},
							"reviews": {
								"dynamic": true,
								"enabled": true
							}
						}
					}
				}
			},
			"store": {
				"indexType": "scorch",
				"kvStoreName": ""
			}
		}
	}`)

	var id *cbgt.IndexDef
	err := json.Unmarshal(sampleIndexDef, &id)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := SearchableFieldsForIndexDef(id)
	if got == nil {
		t.Fatalf("expected a set of searchable fields")
	}

	for _, value := range got {
		sort.Strings(value)
	}

	expect := map[string][]string{}
	expect["hotel"] = []string{"country", "reviews.author", "reviews.content", "state"}
	expect["landmark"] = []string{"_all", "country"}
	expect["default"] = []string{"_all"}

	if !reflect.DeepEqual(expect, got) {
		t.Fatalf("Expected: %v, Got: %v", expect, got)
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
		fields, err := FetchFieldsToSearch(qBytes)
		if err != nil {
			t.Fatal(err)
		}

		sort.Strings(fields)
		if !reflect.DeepEqual(test.expect, fields) {
			t.Fatalf("Expected: %v, Got: %v", test.expect, fields)
		}
	}
}
