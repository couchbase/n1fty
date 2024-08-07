// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
		gotQueryFields, _, _, _, _, err := ParseQueryToSearchRequest("", q)
		if err != nil {
			t.Fatal(test, err)
		}

		if !reflect.DeepEqual(expectQueryFields, gotQueryFields) {
			t.Fatal(test, gotQueryFields)
		}
	}
}

func TestFlexQueryNeedsFiltering(t *testing.T) {
	for _, qStr := range []string{
		`{"match_all": {}}`,
		`{"query": "-whatever"}`,
		`{"must_not": {"disjuncts": [{"query": "whatever"}]}}`,
		`{"conjuncts": [{"match": "whatever"}, {"match_all": {}}]}`,
		`{"conjuncts": [{"match": "whatever"}, {"query": "-whatever"}]}`,
		`{"disjuncts": [{"match": "whatever"}, {"match_all": {}}]}`,
		`{"disjuncts": [{"match": "whatever"}, {"query": "-whatever"}]}`,
	} {
		var qMap map[string]interface{}
		if err := json.Unmarshal([]byte(qStr), &qMap); err != nil {
			t.Fatal(err)
		}
		if ok, err := FlexQueryNeedsFiltering(qMap); !ok || err != nil {
			t.Error(ok, err)
		}
	}
}
