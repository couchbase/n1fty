//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
	"sort"
	"testing"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

func TestParseXattrs(t *testing.T) {

	// qo := expression.NewObjectConstruct(
	// 	algebra.MapPairs(
	// 		[]*algebra.Pair{
	// 			algebra.NewPair(
	// 				expression.NewConstant(value.NewValue("query")),
	// 				expression.NewConstant(value.NewValue("")),
	// 				nil,
	// 			),
	// 		},
	// 	),
	// )

	tests := []struct {
		query  expression.Expression
		xattrs []string
	}{
		{
			query:  expression.NewConstant(value.NewValue("abc")),
			xattrs: []string{},
		},
		{
			query:  expression.NewConstant(value.NewValue("_$xattrs:abc")),
			xattrs: []string{},
		},
		{
			query:  expression.NewConstant(value.NewValue("$xattrs:abc")),
			xattrs: []string{},
		},
		{
			query:  expression.NewConstant(value.NewValue("xyz._$xattrs:abc")),
			xattrs: []string{},
		},
		{
			query:  expression.NewConstant(value.NewValue("_$xattrs.xyz:abc")),
			xattrs: []string{"xyz"},
		},
		{
			query:  expression.NewConstant(value.NewValue("+_$xattrs.xyz:abc -def:hij")),
			xattrs: []string{"xyz"},
		},
		{
			query:  expression.NewConstant(value.NewValue("-_$xattrs.xyz:abc +_$xattrs.def:hij")),
			xattrs: []string{"xyz", "def"},
		},
		{
			query:  expression.NewConstant(value.NewValue("_$xattrs.xyz:abc _$xattrs.xyz:hij")),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("abc")),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("_$xattrs:abc")),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("$xattrs:abc")),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("xyz._$xattrs:abc")),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("_$xattrs.xyz:abc")),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("+_$xattrs.xyz:abc -def:hij")),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("-_$xattrs.xyz:abc +_$xattrs.def:hij")),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz", "def"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewConstant(value.NewValue("_$xattrs.xyz:abc _$xattrs.xyz:hij")),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("abc")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("_$xattrs:abc")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("$xattrs:abc")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("xyz._$xattrs:abc")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("_$xattrs.xyz:abc")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("+_$xattrs.xyz:abc -def:hij")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("-_$xattrs.xyz:abc +_$xattrs.def:hij")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz", "def"},
		},
		{
			query: expression.NewObjectConstruct(
				algebra.MapPairs(
					[]*algebra.Pair{
						algebra.NewPair(
							expression.NewConstant(value.NewValue("query")),
							expression.NewObjectConstruct(
								algebra.MapPairs(
									[]*algebra.Pair{
										algebra.NewPair(
											expression.NewConstant(value.NewValue("query")),
											expression.NewConstant(value.NewValue("_$xattrs.xyz:abc _$xattrs.xyz:hij")),
											nil,
										),
									},
								),
							),
							nil,
						),
					},
				),
			),
			xattrs: []string{"xyz"},
		},
	}

	for i, test := range tests {
		xattrs, err := ParseXattrs(test.query)
		if err != nil {
			t.Fatalf("Unexpected error in testcase %d, query %s, err: %v", i, test.query, err)
		}

		sort.Strings(xattrs)
		sort.Strings(test.xattrs)

		if len(xattrs) != len(test.xattrs) {
			t.Fatalf("Expected %v, got %v, testcase %d", test.xattrs, xattrs, i)
		}

		for i := 0; i < len(test.xattrs); i++ {
			if xattrs[i] != test.xattrs[i] {
				t.Fatalf("Expected %v, got %v, testcase %d", test.xattrs, xattrs, i)
			}
		}
	}
}
