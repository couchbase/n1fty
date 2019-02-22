//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package whitebox

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/analysis/datetime/flexible"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/registry"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/planner"
	"github.com/couchbase/query/server"

	"github.com/couchbase/n1fty/flex"
)

// These tests seem to break in buildbot environment.
// To execute these tests in a local dev environment...
//
//   WHITEBOX=y go test ./flex/whitebox
//
func checkSkipTest(t *testing.T) bool {
	if os.Getenv("WHITEBOX") == "y" {
		return false
	}

	t.Skip()

	fmt.Println("use WHITEBOX=y environment variable to enable")

	return true
}

func init() {
	// Needed for BleveToFlexIndex() to work on dynamic indexes.
	registry.RegisterDateTimeParser("disabled",
		func(config map[string]interface{}, cache *registry.Cache) (analysis.DateTimeParser, error) {
			return flexible.New(nil), nil // With no layouts, "disabled" always return error.
		})
}

func initIndexesById(t *testing.T, m map[string]*Index) map[string]*Index {
	for id, idx := range m {
		if idx.IndexMapping == nil {
			continue
		}

		j, err := json.Marshal(idx.IndexMapping)
		if err != nil {
			t.Fatalf("initIndexesById, json.Marshal, err: %v", err)
		}

		idx.IndexMapping = bleve.NewIndexMapping()

		err = json.Unmarshal(j, &idx.IndexMapping)
		if err != nil {
			t.Fatalf("initIndexesById, json.Unmarshal, err: %v", err)
		}

		if idx.FlexIndex == nil {
			idx.FlexIndex, err = flex.BleveToFlexIndex(idx.IndexMapping)
			if err != nil {
				t.Fatalf("initIndexesById, id: %v, BleveToFlexIndex err: %v", id, err)
				return nil
			}
		}
	}

	return m
}

func emitExpr(t *testing.T, e expression.Expression) {
	fmt.Printf("==========\n")
	fmt.Printf("e: %+v\n", e)

	f, ok := e.(expression.Function)
	if !ok {
		return
	}

	fmt.Printf(" f.Name(): %v\n", f.Name())

	dnf := planner.NewDNF(e, false, true)
	eDNF, err := dnf.Map(e)
	if err != nil {
		t.Errorf("did not expect dnf err: %v", err)
	}

	fmt.Printf(" eDNF: %+v\n", eDNF)
}

func TestParse(t *testing.T) {
	e, _ := parser.Parse("object_pairs('$1') AND x > 1 AND 1 < x")
	emitExpr(t, e)

	e, _ = parser.Parse("x = 1")
	emitExpr(t, e)

	e, _ = parser.Parse("x > 10 AND y > 20 AND (z > 30 OR w > 40)")
	emitExpr(t, e)

	e, _ = parser.Parse("x > 10 AND y > 20 AND (z > 30 OR w > 40) AND (zz > 300 OR ww > 400)")
	emitExpr(t, e)

	e, _ = parser.Parse("(x > 10 AND y > 20) AND (z > 30 OR w > 40) AND (zz > 300 OR ww > 400)")
	emitExpr(t, e)

	e, _ = parser.Parse("(x > 10 AND y > 20) AND (z > 30 OR w > 40) AND (zz > 300 OR ww > 400) OR true")
	emitExpr(t, e)

	e, _ = parser.Parse("x > 10 AND y > 20 AND IFNULL(z > 30 OR w > 40, NULL)")
	emitExpr(t, e)

	e, _ = parser.Parse("x > 10 AND y > 20 AND IFNULL(NULL, NULL)")
	emitExpr(t, e)

	e, _ = parser.Parse("x > 10 AND x > 11 AND x > 12 AND x > 13 AND x > 14")
	emitExpr(t, e)
}

func TestParseSelectFrom(t *testing.T) {
	s, err := n1ql.ParseStatement("SELECT * FROM b WHERE b.x > 10")
	if err != nil {
		t.Errorf("got err: %v", err)
	}
	fmt.Printf("==========\n")
	fmt.Printf("s: %+v\n", s)

	s, err = n1ql.ParseStatement("SELECT * FROM b JOIN c ON b.f = c.f WHERE b.x > 10")
	if err != nil {
		t.Errorf("got err: %v", err)
	}
	fmt.Printf("==========\n")
	fmt.Printf("s: %+v\n", s)

	s, err = n1ql.ParseStatement("SELECT * FROM b JOIN c ON b.f = c.f LET z = c.x WHERE b.x > 10 AND z > 10")
	if err != nil {
		t.Errorf("got err: %v", err)
	}
	fmt.Printf("==========\n")
	fmt.Printf("s: %+v\n", s)
}

func TestSelect1(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	s, err := NewServer("./", nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s, "select 1", nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)

	r, err = ExecuteStatement(s, "select 1 + 2 as three", nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)
}

func TestSelectStarFromDataEmpty(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), nil)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s, "select * from data:empty", nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)
}

func TestSelectStarFromData1Doc(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), nil)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s, "select * from data:`1doc`", nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)
}

func TestSearchWithEmptyIndexes(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), nil)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s,
		"select * from data:`1doc` as b"+
			` WHERE SEARCH(b.a, "hello", {"index": "ftsIdx"})`, nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)
}

func TestNotSargable(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	initIndexer := func(indexer *Indexer) (*Indexer, errors.Error) {
		if indexer.IndexesById == nil {
			indexer.IndexesById = map[string]*Index{
				"ftsIdx": {
					Parent:  indexer,
					IdStr:   "ftsIdx",
					NameStr: "ftsIdx",
				},
			}
		}

		return indexer, nil
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), initIndexer)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s,
		"select * from data:`1doc` as b"+
			` WHERE SEARCH(b.a, "hello", {"index": "ftsIdx"})`, nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)

	r, err = ExecuteStatement(s,
		"select *, META() from data:`1doc` as b", nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)

	r, err = ExecuteStatement(s,
		"select * from data:`1doc` as b"+
			` WHERE SEARCH(b.a, {"match": "hello"}, {"index": "ftsIdx"})`, nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)

	r, err = ExecuteStatement(s,
		"select * from data:`1doc` as b UNNEST children as c UNNEST c.pets as cpets"+
			" LET x = c.pets"+
			` WHERE SEARCH(b.a, {"match": "hello"}, {"index": "ftsIdx"})`+
			`   AND x = "fluffy"`+
			`   AND cpets = "spot"`,
		nil, nil)
	if err != nil {
		t.Errorf("did not expect err: %v", err)
	}

	fmt.Printf("r: %+v\n", r)
}

func TestOrdersData(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	indexesById := map[string]*Index{}

	initIndexer := func(indexer *Indexer) (*Indexer, errors.Error) {
		if indexer.IndexesById == nil {
			indexer.IndexesById = initIndexesById(t, map[string]*Index{
				"ftsIdx": {
					Parent:  indexer,
					IdStr:   "ftsIdx",
					NameStr: "ftsIdx",

					IndexMapping: &mapping.IndexMappingImpl{
						DefaultAnalyzer:       "keyword",
						DefaultDateTimeParser: "dateTimeOptional",
						DefaultMapping: &mapping.DocumentMapping{
							Enabled: true,
							Properties: map[string]*mapping.DocumentMapping{
								"custId": {
									Enabled: true,
									Fields: []*mapping.FieldMapping{
										{
											Name:     "custId",
											Type:     "text",
											Analyzer: "keyword",
											Index:    true,
										},
									},
								},
								"orderlines": {
									Enabled: true,
									Properties: map[string]*mapping.DocumentMapping{
										"productId": {
											Enabled: true,
											Fields: []*mapping.FieldMapping{
												{
													Name:     "productId",
													Type:     "text",
													Analyzer: "keyword",
													Index:    true,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			})

			for id, v := range indexer.IndexesById {
				indexesById[id] = v
			}
		}

		return indexer, nil
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), initIndexer)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	testOrdersData(t, s, indexesById, []testOrdersDataCase{
		{
			`SELECT *
               FROM data:orders as o UNNEST o.orderlines as orderline
              WHERE orderline.productId = "sugar22"`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
			},
			true,
			`{"field":"orderlines.productId","term":"sugar22"}`,
		},
		{
			`SELECT *
               FROM data:orders as o UNNEST o.orderlines as orderline
              WHERE orderline.productId = "sugar22"
                    AND (o.custId = "ccc" OR o.custId = "abc")`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
				flex.FieldTrack("custId"):               2,
			},
			true,
			`{"conjuncts":[{"field":"orderlines.productId","term":"sugar22"},{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o UNNEST orderlines as orderline
                    LEFT OUTER JOIN [] as o2 ON o.id = o2.id
              WHERE o.custId = "ccc" OR o.custId = "abc"`,
			6,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			true,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o
                    LEFT OUTER JOIN [] as o2 ON o.id = o2.id
                    UNNEST o.orderlines as orderline
                LET c = o.custId
              WHERE c = "ccc" OR c = "abc"`,
			6,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			true,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},
	})
}

func TestOrdersDataDynamicIndex(t *testing.T) {
	if checkSkipTest(t) {
		return
	}

	indexesById := map[string]*Index{}

	initIndexer := func(indexer *Indexer) (*Indexer, errors.Error) {
		if indexer.IndexesById == nil {
			indexer.IndexesById = initIndexesById(t, map[string]*Index{
				"ftsIdx": {
					Parent:  indexer,
					IdStr:   "ftsIdx",
					NameStr: "ftsIdx",

					IndexMapping: &mapping.IndexMappingImpl{
						DefaultAnalyzer:       "keyword",
						DefaultDateTimeParser: "disabled",
						DefaultMapping: &mapping.DocumentMapping{
							Enabled: true,
							Dynamic: true,
						},
						IndexDynamic: true,
					},
				},
			})

			for id, v := range indexer.IndexesById {
				indexesById[id] = v
			}
		}

		return indexer, nil
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), initIndexer)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	testOrdersData(t, s, indexesById, []testOrdersDataCase{
		{
			`SELECT *
               FROM data:orders as o UNNEST o.orderlines as orderline
              WHERE orderline.productId = "sugar22"`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
			},
			false,
			`{"field":"orderlines.productId","term":"sugar22"}`,
		},
		{
			`SELECT *
               FROM data:orders as o UNNEST o.orderlines as orderline
              WHERE orderline.productId = "sugar22"
                    AND (o.custId = "ccc" OR o.custId = "abc")`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
				flex.FieldTrack("custId"):               2,
			},
			false,
			`{"conjuncts":[{"field":"orderlines.productId","term":"sugar22"},{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o UNNEST orderlines as orderline
                    LEFT OUTER JOIN [] as o2 ON o.id = o2.id
              WHERE o.custId = "ccc" OR o.custId = "abc"`,
			6,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o
                    LEFT OUTER JOIN [] as o2 ON o.id = o2.id
                    UNNEST o.orderlines as orderline
                LET c = o.custId
              WHERE c = "ccc" OR c = "abc"`,
			6,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},

		// ---------------------------------------------------------------

		{
			`SELECT *
               FROM data:orders as o
              WHERE ANY ol IN o.orderlines
                        SATISFIES ol.instructions = "expedite" END`,
			0,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.instructions"): 1,
			},
			false,
			`{"field":"orderlines.instructions","term":"expedite"}`,
		},
		{
			`SELECT *
               FROM data:orders as o
              WHERE ANY ol IN o.orderlines
                        SATISFIES ol.qty = 100 END`,
			0,
			flex.FieldTracks{},
			false,
			``,
		},
	})
}

type testOrdersDataCase struct {
	stmt                 string
	expectNumResults     int
	expectFieldTracks    flex.FieldTracks
	expectNeedsFiltering bool
	expectBleveQuery     string
}

func testOrdersData(t *testing.T, s *server.Server, indexesById map[string]*Index,
	moreTests []testOrdersDataCase) {
	if len(indexesById) > 0 {
		t.Fatalf("expected empty indexesById")
	}

	tests := append([]testOrdersDataCase{
		{
			`SELECT *, META() as META from data:orders as o WHERE custId = "ccc"`,
			2,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 1,
			},
			false,
			`{"field":"custId","term":"ccc"}`,
		},
		{
			`SELECT *, META() as META FROM data:orders as o
              WHERE custId = "ccc" OR custId = "ddd"`,
			2,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"ddd"}]}`,
		},
		{
			`SELECT *, META() as META FROM data:orders as o
              WHERE custId = "ccc" OR custId = "abc"`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},
		{
			`SELECT *, META() as META FROM data:orders as o
              WHERE ANY orderline IN o.orderlines
                        SATISFIES orderline.productId = "sugar22" END`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
			},
			false,
			`{"field":"orderlines.productId","term":"sugar22"}`,
		},
		{
			`SELECT *, META() as META FROM data:orders as o
              WHERE ANY orderline IN o.orderlines
                        SATISFIES orderline.productId = "sugar22" END
                    AND (o.custId = "ccc" OR o.custId = "abc")`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("orderlines.productId"): 1,
				flex.FieldTrack("custId"):               2,
			},
			false,
			`{"conjuncts":[{"field":"orderlines.productId","term":"sugar22"},{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o LEFT OUTER JOIN [] as o2 ON o.id = o2.id
              WHERE o.custId = "ccc" OR o.custId = "abc"`,
			3,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"disjuncts":[{"field":"custId","term":"ccc"},{"field":"custId","term":"abc"}]}`,
		},
		{
			`SELECT *
               FROM data:orders as o
              WHERE o.custId >= "a" AND o.custId <= "b"`,
			1,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 2,
			},
			false,
			`{"field":"custId","inclusive_max":true,"inclusive_min":true,"max":"b","min":"a"}`,
		},
		{
			`SELECT *
               FROM data:orders as o
              WHERE ISSTRING(o.custId) AND o.custId < "b"`,
			1,
			flex.FieldTracks{
				flex.FieldTrack("custId"): 1,
			},
			false,
			`{"field":"custId","inclusive_max":false,"max":"b"}`,
		},
	}, moreTests...)

	for testi, test := range tests {
		r, err := ExecuteStatement(s, test.stmt, nil, nil)
		if err != nil {
			t.Fatalf("did not expect err: %v", err)
		}
		if len(r) != test.expectNumResults {
			t.Fatalf("test: %+v\n got len(r): %d, r: %+v", test, len(r), r)
		}

		if len(indexesById) != 1 || indexesById["ftsIdx"] == nil {
			t.Fatalf("expected ftsIdx, got: %+v", indexesById)
		}
		idx := indexesById["ftsIdx"]

		last := idx.lastSargableFlexOk
		if last == nil {
			if len(test.expectFieldTracks) <= 0 {
				idx.lastSargableFlexErr = nil
				continue // On to next test if we were expecting not-sargable flex.
			}

			t.Fatalf("testi: %d, test: %+v, expected lastSargableFlexOk",
				testi, test)
		}

		if !reflect.DeepEqual(last.fieldTracks, test.expectFieldTracks) {
			t.Fatalf("test: %+v\n last.fieldTracks (%+v) != test.expectFieldTracks: %+v",
				test, last.fieldTracks, test.expectFieldTracks)
		}

		if last.needsFiltering != test.expectNeedsFiltering {
			t.Fatalf("test: %+v\n last.needsFiltering mismatch: %+v",
				test, last.needsFiltering)
		}

		bleveQueryJson, _ := json.Marshal(last.bleveQuery)
		if string(bleveQueryJson) != test.expectBleveQuery {
			t.Fatalf("test: %+v\n last.bleveQuery mismatch: %s",
				test, bleveQueryJson)
		}

		idx.lastSargableFlexOk = nil
		idx.lastSargableFlexErr = nil
	}
}
