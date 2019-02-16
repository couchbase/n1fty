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
	"encoding/json"
	"reflect"
	"testing"
)

func TestIdentifiersReverseExpand(t *testing.T) {
	tests := []struct {
		identifiers    Identifiers
		identifierName string
		expectOut      []string
		expectOk       bool
	}{
		{Identifiers{}, "foo",
			nil, false},
		{Identifiers{Identifier{Name: "foo"}}, "foo",
			nil, true},
		{
			Identifiers{
				Identifier{Name: "bar"},
				Identifier{Name: "foo"},
			},
			"foo",
			nil,
			true,
		},
		{
			Identifiers{
				Identifier{Name: "bar"},
				Identifier{Name: "foo"},
			},
			"bar",
			nil,
			true,
		},
		{
			Identifiers{
				Identifier{Name: "bar"},
				Identifier{Name: "foo"},
			},
			"miss",
			nil,
			false,
		},
		{
			Identifiers{
				Identifier{Name: "bar", Expansion: []string{"barx"}},
				Identifier{Name: "foo"},
			},
			"bar",
			nil,
			false,
		},
		{
			Identifiers{
				Identifier{Name: "bar", Expansion: []string{"foo"}},
				Identifier{Name: "foo"},
			},
			"bar",
			nil,
			true,
		},
		{
			Identifiers{
				Identifier{
					Name:      "bar",
					Expansion: []string{"foo", "barx"}},
				Identifier{Name: "foo"},
			},
			"bar",
			[]string{"barx"},
			true,
		},
		{
			Identifiers{
				Identifier{
					Name:      "bar",
					Expansion: []string{"foo", "barx", "bary"}},
				Identifier{Name: "foo"},
			},
			"bar",
			[]string{"bary", "barx"},
			true,
		},
		{
			Identifiers{
				Identifier{
					Name:      "w",
					Expansion: []string{"bar", "w1", "w2"}},
				Identifier{
					Name:      "bar",
					Expansion: []string{"foo", "barx", "bary"}},
				Identifier{Name: "foo"},
			},
			"w",
			[]string{"w2", "w1", "bary", "barx"},
			true,
		},
	}

	for _, test := range tests {
		out, ok := test.identifiers.ReverseExpand(
			test.identifierName, nil)
		if ok != test.expectOk {
			t.Errorf("test: %+v, ok mismatch, got: %v",
				test, ok)
		}

		if !reflect.DeepEqual(out, test.expectOut) {
			t.Errorf("test: %+v, out mismatch, got: %+v",
				test, out)
		}
	}
}

func TestFieldInfosFind(t *testing.T) {
	fieldInfos2 := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
		&FieldInfo{FieldPath: []string{"b"}},
	}

	fieldInfosABM := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
		&FieldInfo{FieldPath: []string{"b"}},
		&FieldInfo{FieldPath: []string{"m", "n"}},
	}

	fieldInfosTopLevelDynamic := FieldInfos{
		&FieldInfo{FieldPath: []string{"b"}},
		&FieldInfo{FieldPath: nil},
	}

	fieldInfosTopLevelDynamic2 := FieldInfos{
		&FieldInfo{FieldPath: []string{"b"}},
		&FieldInfo{FieldPath: nil},
	}

	tests := []struct {
		exprStr         string
		fieldInfos      FieldInfos
		expectFieldInfo *FieldInfo
		expectSuffix    []string
	}{
		{"x", fieldInfos2, nil, nil},
		{"a", fieldInfos2, &FieldInfo{FieldPath: []string{"a"}}, nil},
		{"b", fieldInfos2, &FieldInfo{FieldPath: []string{"b"}}, nil},
		{"a.j", fieldInfos2, &FieldInfo{FieldPath: []string{"a"}}, []string{"j"}},
		{"a.j.k", fieldInfos2, &FieldInfo{FieldPath: []string{"a"}}, []string{"j", "k"}},

		{"x[0].j", fieldInfos2, nil, nil},
		{"x.j[0].k", fieldInfos2, nil, nil},

		{"x[0]", fieldInfos2, nil, nil},
		{"x.j[0]", fieldInfos2, nil, nil},

		{"a[0]", fieldInfos2, nil, nil},
		{"a[123].j", fieldInfos2, nil, nil},

		{"a.j[0]", fieldInfos2, nil, nil},
		{"a.j[0].k", fieldInfos2, nil, nil},

		{"a[b]", fieldInfos2, nil, nil},
		{"a.j[b]", fieldInfos2, nil, nil},
		{"a.j[b].k", fieldInfos2, nil, nil},

		{"m", fieldInfosABM, nil, nil},
		{"m.o", fieldInfosABM, nil, nil},
		{"m[0]", fieldInfosABM, nil, nil},

		{"m.n", fieldInfosABM, &FieldInfo{FieldPath: []string{"m", "n"}}, nil},
		{"m.n.a", fieldInfosABM, &FieldInfo{FieldPath: []string{"m", "n"}}, []string{"a"}},
		{"m.n[123]", fieldInfosABM, nil, nil},
		{"m.n[123].x", fieldInfosABM, nil, nil},

		{"m.n[ROUND(1)]", fieldInfosABM, nil, nil},
		{"m.n[ROUND(1)].x", fieldInfosABM, nil, nil},

		{"a", fieldInfosTopLevelDynamic, &FieldInfo{FieldPath: nil}, []string{"a"}},
		{"b", fieldInfosTopLevelDynamic, &FieldInfo{FieldPath: []string{"b"}}, nil},

		{"a", fieldInfosTopLevelDynamic2, &FieldInfo{FieldPath: nil}, []string{"a"}},
		{"b", fieldInfosTopLevelDynamic2, &FieldInfo{FieldPath: []string{"b"}}, nil},
	}

	for _, test := range tests {
		stmt := "SELECT * FROM `bucket` WHERE " + test.exprStr

		s := parseStatement(t, stmt)
		if s == nil {
			t.Errorf("expected s")
		}

		expr := s.Where()

		fieldInfo, suffix := test.fieldInfos.Find(
			Identifiers{Identifier{Name: "bucket"}}, expr, nil)

		if !reflect.DeepEqual(fieldInfo, test.expectFieldInfo) {
			t.Fatalf("test: %+v, mismatch fieldInfo: %v", test, fieldInfo)
		}

		if !reflect.DeepEqual(suffix, test.expectSuffix) {
			t.Fatalf("test: %+v, mismatch suffix: %#v", test, suffix)
		}
	}
}

func TestCheckFieldsUsed(t *testing.T) {
	fieldInfos0 := FieldInfos{}

	fieldInfos1 := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
	}

	fieldInfos2 := FieldInfos{
		&FieldInfo{FieldPath: []string{"a"}},
		&FieldInfo{FieldPath: []string{"b"}},
	}

	fieldInfosTopLevelDynamic := FieldInfos{
		&FieldInfo{FieldPath: []string{"b"}},
		&FieldInfo{FieldPath: nil},
	}

	tests := []struct {
		exprStr    string
		fieldInfos FieldInfos
		expect     bool
	}{
		{"123", fieldInfos0, false},
		{"123", fieldInfos1, false},
		{"123", fieldInfos2, false},

		{"a OR b", fieldInfos0, false},
		{"a OR b", fieldInfos1, true},
		{"a OR b", fieldInfos2, true},

		{"123 AND (a OR b)", fieldInfos0, false},
		{"123 AND (a OR b)", fieldInfos1, true},
		{"123 AND (a OR b)", fieldInfos2, true},

		{"x OR y", fieldInfos0, false},
		{"x OR y", fieldInfos1, false},
		{"x OR y", fieldInfos2, false},

		{"CONCAT(x, a)", fieldInfos0, false},
		{"CONCAT(x, a)", fieldInfos1, true},
		{"CONCAT(x, b)", fieldInfos1, false},
		{"CONCAT(x, b)", fieldInfos2, true},

		{"CONCAT(x, a IS VALUED)", fieldInfos0, false},
		{"CONCAT(x, a IS VALUED)", fieldInfos1, true},
		{"CONCAT(x, b IS VALUED)", fieldInfos1, false},
		{"CONCAT(x, b IS VALUED)", fieldInfos2, true},

		{"CONCAT(x, y OR a IS VALUED)", fieldInfos0, false},
		{"CONCAT(x, y OR a IS VALUED)", fieldInfos1, true},
		{"CONCAT(x, y OR b IS VALUED)", fieldInfos1, false},
		{"CONCAT(x, y OR b IS VALUED)", fieldInfos2, true},

		{"a", fieldInfos0, false},
		{"a", fieldInfos1, true},
		{"a OR x", fieldInfos1, true},
		{"x OR a", fieldInfos1, true},

		// For dynamic indexing, test prefix matches.
		{"a.x", fieldInfos1, true},
		{"a.x.y", fieldInfos1, true},
		{"x.a", fieldInfos1, false},

		// For top-level dynamic indexing, test prefix matches.
		{"a", fieldInfosTopLevelDynamic, true},
		{"a.x", fieldInfosTopLevelDynamic, true},
		{"a.x.y", fieldInfosTopLevelDynamic, true},
		{"x.a", fieldInfosTopLevelDynamic, true},
		{"b", fieldInfosTopLevelDynamic, true},

		{"ROUND(a)", fieldInfos1, true},
		{"ROUND(a.x)", fieldInfos1, true},
		{"ROUND(a.x.y)", fieldInfos1, true},
		{"ROUND(a.x.y) > 0", fieldInfos1, true},

		{"ROUND(x)", fieldInfos1, false},
		{"ROUND(x.a)", fieldInfos1, false},

		{"ROUND(a[0])", fieldInfos1, true},
		{"ROUND(a[0].x)", fieldInfos1, true},
		{"ROUND(a.x[10])", fieldInfos1, true},

		{"a[10]", fieldInfos1, true},
		{"a.x[10]", fieldInfos1, true},

		{"a[x]", fieldInfos1, true},
		{"a[1 + 2]", fieldInfos1, true},

		{"a.b[x]", fieldInfos1, true},
		{"a.b[1 + 2]", fieldInfos1, true},

		{"a.b[ROUND(x)]", fieldInfos1, true},
		{"a.b[ROUND(1 + 2)]", fieldInfos1, true},

		{"b[ROUND(1 + 2)]", fieldInfos1, false},
		{"b.x[ROUND(1 + 2)]", fieldInfos1, false},

		{"b[ROUND(a)]", fieldInfos1, true},
		{"b.x[ROUND(1 + 2 + a)]", fieldInfos1, true},
	}

	for _, test := range tests {
		stmt := "SELECT * FROM `bucket` WHERE " + test.exprStr

		s := parseStatement(t, stmt)
		if s == nil {
			t.Errorf("expected s")
		}

		expr := s.Where()

		j, _ := json.Marshal(test.fieldInfos)

		found, err := CheckFieldsUsed(test.fieldInfos,
			Identifiers{Identifier{Name: "bucket"}}, expr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if found != test.expect {
			t.Fatalf("found (%t) != test.expect (%#v), fieldInfos: %s, expr: %v",
				found, test, j, expr)
		}

		found, err = CheckFieldsUsed(test.fieldInfos,
			Identifiers{Identifier{Name: "wrongBucket"}}, expr)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if found {
			t.Fatalf("expecting not found due to wrong bucket")
		}
	}
}

func TestExpressionFieldPathSuffix(t *testing.T) {
	tests := []struct {
		expr         string
		identifier   string
		prefix       []string
		expectSuffix []string
		expectOk     bool
	}{
		{"`bucket`.foo.bar", "bucket", []string{"foo"},
			[]string{"bar"}, true},
		{"`bucket`.foo", "bucket", []string{"foo"},
			nil, true},
		{"`bucket`.bar", "bucket", []string{"foo"},
			nil, false},
		{"`bucket`.foo", "bucket", []string{"foo", "bar"},
			nil, false},
		{"`bucket`.foo.bar", "bucket", []string{"foo", "bar"},
			nil, true},
		{"`bucket`.foo.bar.baz", "bucket", []string{"foo"},
			[]string{"bar", "baz"}, true},
		{"`bucket`.foo.bar.baz", "bucket", []string{"fuz"},
			nil, false},
		{"`bucket`.foo.bar.baz", "whoops", []string{"foo"},
			nil, false},
		{"`bucket`.foo.bar.baz", "whoops", []string{"foo", "bar"},
			nil, false},
		{"`bucket`.foo.bar", "bucket", []string{"foo", "bar", "baz"},
			nil, false},

		{"`bucket`.foo", "bucket", []string{},
			[]string{"foo"}, true},
		{"`bucket`.foo.bar", "bucket", []string{},
			[]string{"foo", "bar"}, true},

		{"`bucket`.foo[1]", "bucket", []string{},
			nil, false},
		{"`bucket`.foo[ROUND(1 + 2)]", "bucket", []string{},
			nil, false},
		{"`bucket`.foo.bar[ROUND(1 + 2)]", "bucket", []string{},
			nil, false},
		{"`bucket`.foo.bar[ROUND(1 + 2)].baz", "bucket", []string{},
			nil, false},
	}

	for _, test := range tests {
		stmt := "SELECT * FROM `bucket` WHERE " + test.expr

		s := parseStatement(t, stmt)
		if s == nil {
			t.Errorf("expected s")
		}

		expr := s.Where()

		suffix, ok := ExpressionFieldPathSuffix(
			Identifiers{Identifier{Name: test.identifier}},
			expr, test.prefix, nil)

		if ok != test.expectOk {
			t.Errorf("test: %+v, mismatch ok: %v", test, ok)
		}

		if !reflect.DeepEqual(suffix, test.expectSuffix) {
			t.Errorf("test: %+v, mismatch suffix: %+v", test, suffix)
		}
	}
}
