// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package verify

import (
	"encoding/json"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/couchbase/cbft"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/value"
)

func TestVerifyResultWithIndexOption(t *testing.T) {
	q := struct {
		field   string
		query   value.Value
		options value.Value
	}{
		field: "",
		query: value.NewValue(`+name:"stark" +dept:"hand"`),
		options: value.NewValue(map[string]interface{}{
			"index": "temp",
		}),
	}

	tests := []struct {
		input  []byte
		expect bool
	}{
		{
			input:  []byte(`{"dept": "queen", "name": "cersei lannister"}`),
			expect: false,
		},
		{
			input:  []byte(`{"dept": "kings guard", "name": "jaime lannister"}`),
			expect: false,
		},
		{
			input:  []byte(`{"dept": "hand", "name": "eddard stark"}`),
			expect: true,
		},
		{
			input:  []byte(`{"dept": "king", "name": "robert baratheon"}`),
			expect: false,
		},
	}

	util.SetIndexMapping("temp", &util.MappingDetails{
		SourceName: "temp_keyspace",
		IMapping:   bleve.NewIndexMapping(),
	})

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options, 1)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		got, err := v.Evaluate(value.NewValue(test.input))
		if err != nil {
			t.Fatal(err)
		}

		if got != test.expect {
			t.Fatalf("Expected: %v, Got: %v, for doc: %v",
				test.expect, got, string(test.input))
		}
	}
}

func TestVerifyResultWithoutIndexOption(t *testing.T) {
	q := struct {
		field   string
		query   value.Value
		options value.Value
	}{
		field: "",
		query: value.NewValue(map[string]interface{}{
			"match":    "2019-03-21 12:00:00",
			"field":    "details.startDate",
			"analyzer": "keyword",
		}),
		options: nil,
	}

	test := struct {
		input  []byte
		expect bool
	}{
		input:  []byte(`{"details": {"startDate": "2019-03-21 12:00:00"}}`),
		expect: true,
	}

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options, 1)
	if err != nil {
		t.Fatal(err)
	}

	got, err := v.Evaluate(value.NewValue(test.input))
	if err != nil {
		t.Fatal(err)
	}

	if got != test.expect {
		t.Fatalf("Expected: %v, Got %v, for doc: %v",
			test.expect, got, string(test.input))
	}
}

func TestNewVerifyWithInvalidIndexUUID(t *testing.T) {
	util.SetIndexMapping("temp", &util.MappingDetails{
		UUID:       "tempUUID",
		SourceName: "temp_keyspace",
		IMapping:   bleve.NewIndexMapping(),
	})

	q := struct {
		field   string
		query   value.Value
		options value.Value
	}{
		field: "",
		query: value.NewValue(`search_term`),
		options: value.NewValue(map[string]interface{}{
			"index":     "temp",
			"indexUUID": "incorrectUUID",
		}),
	}

	vctx, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = vctx.Evaluate(value.NewValue(nil))
	if err == nil {
		t.Fatal(err)
	}
}

func TestMB33444(t *testing.T) {
	q := struct {
		field   string
		query   value.Value
		options value.Value
	}{
		field:   "",
		query:   value.NewValue(`title:encryption`),
		options: nil,
	}

	tests := []struct {
		input  []byte
		expect bool
	}{
		{
			input:  []byte(`{"id":"one","title":"Persistent multi-tasking encryption"}`),
			expect: true,
		},
		{
			input:  []byte(`{"id":"two","title":"Persevering modular encryption"}`),
			expect: true,
		},
		{
			input:  []byte(`{"id":"three","title":"encryption"}`),
			expect: true,
		},
	}

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options, 1)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		got, err := v.Evaluate(value.NewValue(test.input))
		if err != nil {
			t.Fatal(err)
		}

		if got != test.expect {
			t.Errorf("Expected: %v, Got: %v, for doc: %v",
				test.expect, got, string(test.input))
		}
	}
}

func TestDocIDQueryEvaluation(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"abhi","city":"san francisco"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key-1"})
	item.SetId("key-1")

	queryVal := value.NewValue(map[string]interface{}{
		"ids": []interface{}{"key-1"},
	})

	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	ret, err := v.Evaluate(item)
	if err != nil {
		t.Fatal(err)
	}

	if !ret {
		t.Fatal("Expected evaluation for key-1 to succeed")
	}
}

func TestMB39592(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"xyz","dept":"Engineering"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	for _, q := range []map[string]interface{}{
		{"match": "xyz", "field": "name"},
		{"wildcard": "Eng?neer?ng", "field": "dept"},
	} {
		queryVal := value.NewValue(q)
		v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		ret, err := v.Evaluate(item)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		if !ret {
			t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
		}
	}
}

func TestMB41536(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"xyz","dept":"Engineering"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	for _, q := range []map[string]interface{}{
		{"wildcard": "Eng?neer?ng"},
	} {
		queryVal := value.NewValue(q)
		v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		ret, err := v.Evaluate(item)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		if !ret {
			t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
		}
	}
}

func TestVerifyEvalWithScopeCollectionMapping(t *testing.T) {
	indexParams := []byte(`{
		"doc_config": {
			"mode": "scope.collection.type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_mapping": {
				"enabled": false
			},
			"type_field": "_type",
			"types": {
				"scope1.collection1.airline": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"country": {
							"enabled": true,
							"dynamic": false,
							"fields": [{
								"name": "country",
								"type": "text",
								"analyzer": "keyword",
								"index": true
							}]
						}
					}
				}
			},
			"store": {
				"indexType": "scorch"
			}
		}
	}`)
	bp := cbft.NewBleveParams()
	err := json.Unmarshal(indexParams, bp)
	if err != nil {
		t.Fatal(err)
	}

	im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		t.Fatal("Unable to set up index mapping")
	}

	util.SetIndexMapping("temp", &util.MappingDetails{
		UUID:         "tempUUID",
		SourceName:   "temp_keyspace",
		IMapping:     im,
		DocConfig:    &bp.DocConfig,
		Scope:        "scope1",
		Collection:   "collection1",
		TypeMappings: []string{"airline"},
	})

	item := value.NewAnnotatedValue([]byte(`{
		"name" : "xyz",
		"country" : "United States",
		"type" : "airline"
	}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	q := value.NewValue(map[string]interface{}{
		"match": "United States",
		"field": "country",
	})

	options := value.NewValue(map[string]interface{}{
		"index": "temp",
	})

	v, err := NewVerify("`temp_keyspace.scope1.collection1`", "", q, options, 1)
	if err != nil {
		t.Fatal(err)
	}

	got, err := v.Evaluate(item)
	if err != nil {
		t.Fatal(err)
	}

	if !got {
		t.Fatal("Expected key to pass evaluation")
	}
}

func TestVerificationForVariousIndexes(t *testing.T) {
	// This test verfies behavior for the following indexes ..
	// - default mapping
	// - _default scope, _default collection, default mapping
	// - _default scope, _default collection, custom type mapping
	// - custom scope, custom collection, default mapping
	// - custom scope, custom collection, custom type mapping
	// - multiple custom scope.collection.type mappings
	//
	// Related: MB46821, MB46547

	tests := []struct {
		indexName      string
		sourceName     string
		indexParams    []byte
		scope          string
		collection     string
		typeMappings   []string
		verifyKeyspace string
	}{
		{
			indexName:  "temp_1",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
				{
					"doc_config": {
						"mode": "type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_field": "_all",
						"default_mapping": {
							"dynamic": true,
							"enabled": true
						},
						"default_type": "_default",
						"type_field": "_type"
					},
					"store": {
						"indexType": "scorch"
					}
				}`),
			verifyKeyspace: "temp_keyspace",
		},
		{
			indexName:  "temp_2",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
			{
				"doc_config": {
					"mode": "scope.collection.type_field",
					"type_field": "type"
				},
				"mapping": {
					"default_analyzer": "standard",
					"default_field": "_all",
					"default_mapping": {
						"enabled": false
					},
					"default_type": "_default",
					"type_field": "_type",
					"types": {
						"_default._default": {
							"dynamic": true,
							"enabled": true
						}
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}`),
			scope:          "_default",
			collection:     "_default",
			verifyKeyspace: "temp_keyspace._default._default",
		},
		{
			indexName:  "temp_3",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
				{
					"doc_config": {
						"mode": "scope.collection.type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_field": "_all",
						"default_mapping": {
							"enabled": false
						},
						"default_type": "_default",
						"type_field": "_type",
						"types": {
							"_default._default.typeX": {
								"dynamic": true,
								"enabled": true
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}`),
			scope:          "_default",
			collection:     "_default",
			typeMappings:   []string{"typeX"},
			verifyKeyspace: "temp_keyspace._default._default",
		},
		{
			indexName:  "temp_4",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
				{
					"doc_config": {
						"mode": "scope.collection.type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_field": "_all",
						"default_mapping": {
							"enabled": false
						},
						"default_type": "_default",
						"type_field": "_type",
						"types": {
							"scopeX.collectionX": {
								"dynamic": true,
								"enabled": true
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}`),
			scope:          "scopeX",
			collection:     "collectionX",
			verifyKeyspace: "temp_keyspace.scopeX.collectionX",
		},
		{
			indexName:  "temp_5",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
				{
					"doc_config": {
						"mode": "scope.collection.type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_field": "_all",
						"default_mapping": {
							"enabled": false
						},
						"default_type": "_default",
						"type_field": "_type",
						"types": {
							"scopeX.collectionX.typeX": {
								"dynamic": true,
								"enabled": true
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}`),
			scope:          "scopeX",
			collection:     "collectionX",
			typeMappings:   []string{"typeX"},
			verifyKeyspace: "temp_keyspace.scopeX.collectionX",
		},
		{
			indexName:  "temp_6",
			sourceName: "temp_keyspace",
			indexParams: []byte(`
				{
					"doc_config": {
						"mode": "scope.collection.type_field",
						"type_field": "type"
					},
					"mapping": {
						"default_analyzer": "standard",
						"default_field": "_all",
						"default_mapping": {
							"enabled": false
						},
						"default_type": "_default",
						"type_field": "_type",
						"types": {
							"scopeX.collectionX.typeX": {
								"dynamic": true,
								"enabled": true
							},
							"scopeY.collectionY.typeX": {
								"dynamic": true,
								"enabled": true
							}
						}
					},
					"store": {
						"indexType": "scorch"
					}
				}`),
			scope:          "scopeX",
			collection:     "collectionX",
			typeMappings:   []string{"typeX"},
			verifyKeyspace: "temp_keyspace.scopeX.collectionX",
		},
	}

	item := value.NewAnnotatedValue([]byte(`{
		"fieldX" : "xyz",
		"type": "typeX"
	}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	queries := []value.Value{
		value.NewValue(map[string]interface{}{
			"field": "fieldX",
			"match": "xyz",
		}),
		value.NewValue(map[string]interface{}{
			"match": "xyz",
		}),
	}

	for i := range tests {
		bp := cbft.NewBleveParams()
		err := json.Unmarshal(tests[i].indexParams, bp)
		if err != nil {
			t.Fatalf("[test-%d], err: %v", i+1, err)
		}

		im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
		if !ok {
			t.Fatalf("[test-%d] Unable to set up index mapping", i+1)
		}

		util.SetIndexMapping(tests[i].indexName, &util.MappingDetails{
			SourceName:   tests[i].sourceName,
			IMapping:     im,
			DocConfig:    &bp.DocConfig,
			Scope:        tests[i].scope,
			Collection:   tests[i].collection,
			TypeMappings: tests[i].typeMappings,
		})

		options := value.NewValue(map[string]interface{}{
			"index": tests[i].indexName,
		})

		for _, q := range queries {
			v, err := NewVerify(tests[i].verifyKeyspace, "", q, options, 1)
			if err != nil {
				t.Fatalf("[test-%d], keyspace: %v, query: %v, err: %v",
					i+1, tests[i].verifyKeyspace, q, err)
			}

			got, err := v.Evaluate(item)
			if err != nil {
				t.Errorf("[test-%d], keyspace: %v, query: %v, err: %v",
					i+1, tests[i].verifyKeyspace, q, err)
				continue
			}

			if !got {
				t.Errorf("[test-%d] Expected key to pass evaluation,"+
					" keyspace: %v, query: %v", i+1, tests[i].verifyKeyspace, q)
			}
		}
	}
}

func TestMB46867(t *testing.T) {
	tests := []struct {
		indexName      string
		indexParams    []byte
		verifyKeyspace string
	}{
		{
			indexName: "temp_1",
			indexParams: []byte(`
			{
				"doc_config": {
					"mode": "type_field",
					"type_field": "type"
				},
				"mapping": {
					"default_field": "_all",
					"default_mapping": {
						"enabled": false
					},
					"default_type": "_default",
					"type_field": "_type",
					"types": {
						"emp": {
							"enabled": true,
							"reports": {
								"enabled": true,
								"fields": [
								{
									"index": true,
									"include_in_all": true,
									"name": "reports",
									"type": "text"
								}
								]
							}
						}
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
			`),
			verifyKeyspace: "default",
		},
		{
			indexName: "temp_2",
			indexParams: []byte(`
			{
				"doc_config": {
					"mode": "scope.collection.type_field",
					"type_field": "type"
				},
				"mapping": {
					"default_field": "_all",
					"default_mapping": {
						"enabled": false
					},
					"default_type": "_default",
					"type_field": "_type",
					"types": {
						"_default._default.emp": {
							"enabled": true,
							"reports": {
								"enabled": true,
								"fields": [
								{
									"index": true,
									"include_in_all": true,
									"name": "reports",
									"type": "text"
								}
								]
							}
						}
					}
				},
				"store": {
					"indexType": "scorch"
				}
			}
			`),
			verifyKeyspace: "default._default._default",
		},
	}

	item := value.NewAnnotatedValue([]byte(`{
		"reports" : "xyz",
		"type": "emp"
	}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	queries := []value.Value{
		value.NewValue(map[string]interface{}{
			"field": "reports",
			"match": "xyz",
		}),
		value.NewValue(map[string]interface{}{
			"match": "xyz",
		}),
	}

	for i := range tests {
		bp := cbft.NewBleveParams()
		err := json.Unmarshal(tests[i].indexParams, bp)
		if err != nil {
			t.Fatalf("[test-%d], err: %v", i+1, err)
		}

		im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
		if !ok {
			t.Fatalf("[test-%d] Unable to set up index mapping", i+1)
		}

		util.SetIndexMapping(tests[i].indexName, &util.MappingDetails{
			SourceName:   "default",
			IMapping:     im,
			DocConfig:    &bp.DocConfig,
			Scope:        "_default",
			Collection:   "_default",
			TypeMappings: []string{"emp"},
		})

		options := value.NewValue(map[string]interface{}{
			"index": tests[i].indexName,
		})

		for _, q := range queries {
			v, err := NewVerify(tests[i].verifyKeyspace, "", q, options, 1)
			if err != nil {
				t.Fatalf("[test-%d], keyspace: %v, query: %v, err: %v",
					i+1, tests[i].verifyKeyspace, q, err)
			}

			got, err := v.Evaluate(item)
			if err != nil {
				t.Errorf("[test-%d], keyspace: %v, query: %v, err: %v",
					i+1, tests[i].verifyKeyspace, q, err)
				continue
			}

			if !got {
				t.Errorf("[test-%d] Expected key to pass evaluation,"+
					" keyspace: %v, query: %v", i+1, tests[i].verifyKeyspace, q)
			}
		}
	}
}

func TestMB47265(t *testing.T) {
	// array indexing fails with sear
	item := value.NewAnnotatedValue([]byte(`{"name":"xyz","lang": ["Quechua", "Thai", "Portuguese"]}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	for _, q := range []map[string]interface{}{
		{"match": "Quechua", "field": "lang"},
		{"match": "Thai", "field": "lang"},
		{"match": "Portuguese", "field": "lang"},
	} {
		queryVal := value.NewValue(q)
		v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		ret, err := v.Evaluate(item)
		if err != nil {
			t.Fatal(queryVal, err)
		}

		if !ret {
			t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
		}
	}
}

func TestMB47438(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"xyz","dept":"Engineering"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	q := map[string]interface{}{
		"query": "-Marketing",
	}
	queryVal := value.NewValue(q)
	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	ret, err := v.Evaluate(item)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	if !ret {
		t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
	}
}

func TestMB47473(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"KÓ"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	q := map[string]interface{}{
		"field":    "name",
		"wildcard": "K*",
	}
	queryVal := value.NewValue(q)
	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	ret, err := v.Evaluate(item)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	if !ret {
		t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
	}
}

func TestConcurrentEval(t *testing.T) {
	q := map[string]interface{}{
		"field": "name",
		"match": "abhi",
	}
	queryVal := value.NewValue(q)

	parallelism := 10
	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, parallelism)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	signalCh := make(chan struct{}, parallelism)

	for ii := 0; ii < parallelism; ii++ {
		go func(ch chan struct{}) {
			for jj := 0; jj < 100; jj++ {
				item := value.NewAnnotatedValue([]byte(`{"name":"abhi"}`))
				item.SetAttachment("meta", map[string]interface{}{"id": "key"})
				item.SetId("key")

				ret, err := v.Evaluate(item)
				if err != nil {
					t.Fatal(queryVal, err)
				}
				if !ret {
					t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
				}
			}

			ch <- struct{}{}
		}(signalCh)
	}

	for ii := 0; ii < parallelism; ii++ {
		<-signalCh
	}
}

func TestMB49888(t *testing.T) {
	item := value.NewAnnotatedValue([]byte(`{"name":"xyz"}`))
	item.SetAttachment("meta", map[string]interface{}{"id": "key"})
	item.SetId("key")

	qBytes := []byte(`{"query": {"query": "name:xyz"}, "fields": ["*"]}`)
	var q map[string]interface{}
	err := json.Unmarshal(qBytes, &q)
	if err != nil {
		t.Fatal(err)
	}
	queryVal := value.NewValue(q)
	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil, 1)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	ret, err := v.Evaluate(item)
	if err != nil {
		t.Fatal(queryVal, err)
	}

	if !ret {
		t.Fatalf("Expected evaluation for key to succeed for `%v`", queryVal)
	}
}
