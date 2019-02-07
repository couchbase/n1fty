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

	"github.com/couchbase/query/value"
)

func TestVerifyResult(t *testing.T) {
	q := struct {
		field   string
		query   string
		options string
	}{
		field:   "",
		query:   `+name:"stark" +dept:"hand"`,
		options: "",
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

	v, err := NewVerify("",
		q.field,
		value.NewValue(q.query),
		value.NewValue(q.options))
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
