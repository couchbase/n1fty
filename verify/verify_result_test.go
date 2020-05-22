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

package verify

import (
	"testing"

	"github.com/blevesearch/bleve"
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

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options)
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

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options)
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

	vctx, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options)
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

	v, err := NewVerify("`temp_keyspace`", q.field, q.query, q.options)
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

	v, err := NewVerify("`temp_keyspace`", "", queryVal, nil)
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
		v, err := NewVerify("`temp_keyspace`", "", queryVal, nil)
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
