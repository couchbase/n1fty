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
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/planner"

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
		}

		return indexer, nil
	}

	c := MakeWrapCallbacksForIndexType(datastore.IndexType("FTS"), initIndexer)

	s, err := NewServer("./", c)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}

	r, err := ExecuteStatement(s,
		`select *, META() as META from data:orders as o WHERE custId = "ccc"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 2 {
		t.Fatalf("expected3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *, META() as META FROM data:orders as o
          WHERE custId = "ccc" OR custId = "ddd"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 2 {
		t.Fatalf("expected3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *, META() as META FROM data:orders as o
          WHERE custId = "ccc" OR custId = "abc"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 3 {
		t.Fatalf("expected3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *, META() as META FROM data:orders as o
          WHERE ANY orderline IN o.orderlines
                    SATISFIES orderline.productId = "sugar22" END`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 3 {
		t.Fatalf("expected3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *, META() as META FROM data:orders as o
          WHERE ANY orderline IN o.orderlines
                    SATISFIES orderline.productId = "sugar22" END
            AND (o.custId = "ccc" OR o.custId = "abc")`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 3 {
		t.Fatalf("expected 3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o UNNEST orderlines as orderline
          WHERE orderline.productId = "sugar22"
                AND (o.custId = "ccc" OR o.custId = "abc")`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 3 {
		t.Fatalf("expected 3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o LEFT OUTER JOIN [] as o2 ON o.id = o2.id
          WHERE o.custId = "ccc" OR o.custId = "abc"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 3 {
		t.Fatalf("expected 3, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o UNNEST orderlines as orderline
                LEFT OUTER JOIN [] as o2 ON o.id = o2.id
          WHERE o.custId = "ccc" OR o.custId = "abc"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 6 {
		t.Fatalf("expected 6, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o
                LEFT OUTER JOIN [] as o2 ON o.id = o2.id
                UNNEST o.orderlines as orderline
            LET c = o.custId
          WHERE c = "ccc" OR c = "abc"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 6 {
		t.Fatalf("expected 6, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o
          WHERE o.custId >= "a" AND o.custId <= "b"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 1 {
		t.Fatalf("expected 1, got r: %d: %+v\n\n", len(r), r)
	}

	r, err = ExecuteStatement(s,
		`SELECT *
           FROM data:orders as o
          WHERE ISSTRING(o.custId) AND o.custId < "b"`, nil, nil)
	if err != nil {
		t.Fatalf("did not expect err: %v", err)
	}
	if len(r) != 1 {
		t.Fatalf("expected 1, got r: %d: %+v\n\n", len(r), r)
	}

	fmt.Printf("got r: %d: %+v\n\n", len(r), r)
}
