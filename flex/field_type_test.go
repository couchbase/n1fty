//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package flex

import (
	"reflect"
	"testing"

	"github.com/couchbase/query/planner"
)

func TestProcessConjunctFieldTypes(t *testing.T) {
	var fieldInfosZ FieldInfos // Test nil.

	fieldInfos0 := FieldInfos{}

	fieldInfosA := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
	}

	fieldInfosAB := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
		&FieldInfo{FieldPath: []string{"b"}},
	}

	var fieldTypesZ FieldTypes // Test nil.

	fieldTypes0 := FieldTypes{}

	fieldTypes1a := FieldTypes{
		map[FieldTrack]string{
			FieldTrack("a"): "string",
		},
	}

	fieldTypes1aNumber := FieldTypes{
		map[FieldTrack]string{
			FieldTrack("a"): "number",
		},
	}

	fieldTypes1ab := FieldTypes{
		map[FieldTrack]string{
			FieldTrack("a"): "string",
			FieldTrack("b"): "string",
		},
	}

	fieldTypes1a1b := FieldTypes{
		map[FieldTrack]string{
			FieldTrack("a"): "string",
		},
		map[FieldTrack]string{
			FieldTrack("a"): "string",
		},
	}

	tests := []struct {
		exprStr    string
		fieldInfos FieldInfos
		fieldTypes FieldTypes

		expectOutExprs   string
		expectFieldTypes FieldTypes
		expectOk         bool
	}{
		{"123 AND 234", fieldInfosZ, fieldTypesZ,
			"[123, 234]", fieldTypesZ, true},
		{"123 AND 234", fieldInfos0, fieldTypes0,
			"[123, 234]", fieldTypes0, true},
		{"123 AND 234", fieldInfosA, fieldTypes0,
			"[123, 234]", fieldTypes0, true},
		{"123 AND 234", fieldInfosAB, fieldTypes0,
			"[123, 234]", fieldTypes0, true},

		{"123 AND 234", fieldInfosZ, fieldTypes1a,
			"[123, 234]", fieldTypes1a, true},
		{"123 AND 234", fieldInfosZ, fieldTypes1ab,
			"[123, 234]", fieldTypes1ab, true},
		{"123 AND 234", fieldInfosZ, fieldTypes1a1b,
			"[123, 234]", fieldTypes1a1b, true},

		{`a = "hi" AND a = "bye"`, fieldInfosZ, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`a`) = \"bye\")]",
			fieldTypesZ, true},
		{`a = "hi" AND a = "bye"`, fieldInfos0, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`a`) = \"bye\")]",
			fieldTypesZ, true},
		{`a = "hi" AND a = "bye"`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`a`) = \"bye\")]",
			fieldTypesZ, true},
		{`a = "hi" AND a = "bye"`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`a`) = \"bye\")]",
			fieldTypesZ, true},

		{`ISSTRING(a) AND a = "hi"`, fieldInfosZ, fieldTypesZ,
			"[(\"\" <= (`bucket`.`a`)), ((`bucket`.`a`) < []), ((`bucket`.`a`) = \"hi\")]",
			fieldTypesZ, true},

		{`ISSTRING(a) AND a = "hi"`, fieldInfos0, fieldTypesZ,
			"[(\"\" <= (`bucket`.`a`)), ((`bucket`.`a`) < []), ((`bucket`.`a`) = \"hi\")]",
			fieldTypesZ, true},

		{`ISSTRING(a) AND a = "hi"`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\")]",
			fieldTypes1a, true},

		{`ISSTRING(a) AND a = "hi"`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\")]",
			fieldTypes1a, true},

		{`ISSTRING(a) AND a = "hi" AND c = "C"`, fieldInfosZ, fieldTypesZ,
			"[(\"\" <= (`bucket`.`a`)), ((`bucket`.`a`) < [])," +
				" ((`bucket`.`a`) = \"hi\")," +
				" ((`bucket`.`c`) = \"C\")]",
			fieldTypesZ, true},

		{`ISSTRING(a) AND a = "hi" AND c = "C"`, fieldInfos0, fieldTypesZ,
			"[(\"\" <= (`bucket`.`a`)), ((`bucket`.`a`) < [])," +
				" ((`bucket`.`a`) = \"hi\")," +
				" ((`bucket`.`c`) = \"C\")]",
			fieldTypesZ, true},

		{`ISSTRING(a) AND a = "hi" AND c = "C"`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`c`) = \"C\")]",
			fieldTypes1a, true},

		{`ISSTRING(a) AND a = "hi" AND c = "C"`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`c`) = \"C\")]",
			fieldTypes1a, true},

		{`a = "hi" AND c = "C" AND ISSTRING(a)`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = \"hi\"), ((`bucket`.`c`) = \"C\")]",
			fieldTypes1a, true},

		{`ISNUMBER(a) AND a = 123 AND c = "C"`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = 123), ((`bucket`.`c`) = \"C\")]",
			fieldTypes1aNumber, true},

		{`a = 123 AND c = "C" AND ISNUMBER(a)`, fieldInfosAB, fieldTypesZ,
			"[((`bucket`.`a`) = 123), ((`bucket`.`c`) = \"C\")]",
			fieldTypes1aNumber, true},

		// ------------------------------------------------------

		{`ISSTRING(a) AND a = $namedX`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = $namedX)]",
			fieldTypes1a, true},

		{`ISNUMBER(a) AND a = $namedX`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = $namedX)]",
			fieldTypes1aNumber, true},

		{`ISSTRING(a) AND a = $1`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = $1)]",
			fieldTypes1a, true},

		{`ISNUMBER(a) AND a = $1`, fieldInfosA, fieldTypesZ,
			"[((`bucket`.`a`) = $1)]",
			fieldTypes1aNumber, true},
	}

	for testi, test := range tests {
		stmt := "SELECT * FROM `bucket` WHERE " + test.exprStr

		s := parseStatement(t, stmt)
		if s == nil {
			t.Errorf("expected s")
		}

		exprWhere := s.Where()

		exprWhereSimplified, _ :=
			planner.NewDNF(exprWhere, false, false /* doDNF */).Map(exprWhere)

		outExprs, outFieldTypes, outNeedsFiltering, outOk :=
			ProcessConjunctFieldTypes(
				test.fieldInfos, Identifiers{Identifier{Name: "bucket"}},
				exprWhereSimplified.Children(), test.fieldTypes)
		if outOk != test.expectOk {
			t.Fatalf("testi: %d, expected %+v, got outOk %t",
				testi, test, outOk)
		}

		if outOk {
			if outExprs.String() != test.expectOutExprs {
				t.Fatalf("testi: %d, expected %+v, outExprs: %v",
					testi, test, outExprs.String())
			}

			_ = outNeedsFiltering // TODO.
		}

		if !reflect.DeepEqual(outFieldTypes, test.expectFieldTypes) {
			t.Fatalf("testi: %d, expected %+v, got fieldTypes: %#v",
				testi, test, outFieldTypes)
		}
	}
}
