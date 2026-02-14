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
	"math"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

// Mock vector implementation for testing
type mockVector struct {
	entries []timestamp.Entry
}

func (m *mockVector) Entries() []timestamp.Entry {
	return m.entries
}

type mockEntry struct {
	position uint32
	value    uint64
	guard    string
}

func (m *mockEntry) Position() uint32 {
	return m.position
}

func (m *mockEntry) Value() uint64 {
	return m.value
}

func (m *mockEntry) Guard() string {
	return m.guard
}

func newMockVector(entries []timestamp.Entry) timestamp.Vector {
	return &mockVector{entries: entries}
}

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		field string
		query value.Value
	}{
		{
			field: "title",
			query: value.NewValue(`+Avengers~2 company:marvel`),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match":     "avengers",
				"field":     "title",
				"fuzziness": 2,
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"match_phrase": "Avengers: Infinity War",
				"field":        "title",
				"analyzer":     "en",
				"boost":        10,
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"wildcard": "Avengers*",
				"field":    "title",
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"conjuncts": []interface{}{
					map[string]interface{}{
						"match": "abc",
						"field": "cba",
					},
					map[string]interface{}{
						"match": "xyz",
						"field": "zyx",
					},
				},
			}),
		},
		{
			field: "not-used",
			query: value.NewValue(map[string]interface{}{
				"must": map[string]interface{}{
					"conjuncts": []interface{}{
						map[string]interface{}{
							"match":     "Avengers",
							"field":     "title",
							"fuzziness": 2,
						},
					},
				},
				"filter": map[string]interface{}{
					"conjuncts": []interface{}{
						map[string]interface{}{
							"match": "marvel",
							"field": "company",
						},
					},
				},
			}),
		},
	}

	for i, test := range tests {
		q, err := BuildQuery(test.field, test.query)
		if err != nil {
			t.Fatal(err)
		}

		switch qq := q.(type) {
		case *query.BooleanQuery:
			cq := qq.Must.(*query.ConjunctionQuery)
			if len(cq.Conjuncts) != 1 {
				t.Fatalf("Exception in boolean query, number of must clauses: %v",
					len(cq.Conjuncts))
			}
			mcq := cq.Conjuncts[0].(*query.MatchQuery)
			if mcq.Match != "Avengers" || mcq.FieldVal != "title" || mcq.Fuzziness != 2 {
				t.Fatalf("Exception in boolean must query: %v, %v, %v",
					mcq.Match, mcq.FieldVal, mcq.Fuzziness)
			}
			if qq.Should != nil {
				dq := qq.Should.(*query.DisjunctionQuery)
				if len(dq.Disjuncts) != 1 {
					t.Fatalf("Exception in boolean query, number of should clauses: %v",
						len(dq.Disjuncts))
				}
				mdq := dq.Disjuncts[0].(*query.MatchQuery)
				if mdq.Match != "marvel" || mdq.FieldVal != "company" {
					t.Fatalf("Exception in boolean should query: %v, %v",
						mdq.Match, mdq.FieldVal)
				}
			}
			if qq.Filter != nil {
				fq := qq.Filter.(*query.ConjunctionQuery)
				if len(fq.Conjuncts) != 1 {
					t.Fatalf("Exception in boolean query, number of filter clauses: %v",
						len(fq.Conjuncts))
				}
				mfq := fq.Conjuncts[0].(*query.MatchQuery)
				if mfq.Match != "marvel" || mfq.FieldVal != "company" {
					t.Fatalf("Exception in boolean filter query: %v, %v",
						mfq.Match, mfq.FieldVal)
				}
			}
		case *query.MatchQuery:
			if qq.Match != "avengers" || qq.FieldVal != "title" || qq.Fuzziness != 2 {
				t.Fatalf("Exception in match query: %v, %v, %v",
					qq.Match, qq.FieldVal, qq.Fuzziness)
			}
		case *query.MatchPhraseQuery:
			if qq.MatchPhrase != "Avengers: Infinity War" || qq.FieldVal != "title" ||
				qq.Analyzer != "en" || float64(*qq.BoostVal) != float64(10) {
				t.Fatalf("Exception in match phrase query: %v, %v, %v, %v",
					qq.MatchPhrase, qq.FieldVal, qq.Analyzer, *qq.BoostVal)
			}
		case *query.WildcardQuery:
			if qq.Wildcard != "Avengers*" || qq.FieldVal != "title" {
				t.Fatalf("Exception in wildcard query: %v, %v", qq.Wildcard, qq.FieldVal)
			}
		case *query.ConjunctionQuery:
			if len(qq.Conjuncts) != 2 {
				t.Fatalf("Exception in conjunction query: %v", len(qq.Conjuncts))
			}
			mq1, ok1 := qq.Conjuncts[0].(*query.MatchQuery)
			mq2, ok2 := qq.Conjuncts[1].(*query.MatchQuery)
			if !ok1 || !ok2 || mq1.Field() != "cba" || mq2.Field() != "zyx" {
				t.Fatalf("Exception in conjunction query")
			}
		default:
			t.Fatalf("Unexpected query type: %v, for entry: %v", reflect.TypeOf(q), i)
		}
	}
}

func TestBuildBadQuery(t *testing.T) {
	q := value.NewValue(map[string]interface{}{
		"this": "is",
		"a":    "very",
		"bad":  "example",
	})

	if _, err := BuildQuery("", q); err == nil {
		t.Fatal("Expected an error, but didn't see one")
	}
}

func TestBuildSearchRequest(t *testing.T) {
	tests := []struct {
		id    int
		query value.Value
	}{
		{
			id: 1,
			query: value.NewValue(map[string]interface{}{
				"from": 90,
				"size": 100,
				"query": value.NewValue(map[string]interface{}{
					"wildcard": "Avengers*",
					"field":    "title",
				}),
				"explain":          true,
				"includeLocations": true,
				"fields":           []interface{}{"country", "city"},
				"sort": []interface{}{value.NewValue(map[string]interface{}{
					"by":    "geo_distance",
					"field": "geo",
					"unit":  "mi",
					"location": map[string]interface{}{
						"lon": -2.235143,
						"lat": 53.482358,
					},
				})},
			}),
		},
		{
			id: 2,
			query: value.NewValue(map[string]interface{}{
				"size": -10,
				"query": value.NewValue(map[string]interface{}{
					"match":     "avengers",
					"field":     "title",
					"fuzziness": 2,
				}),
			}),
		},
		{
			id: 3,
			query: value.NewValue(map[string]interface{}{
				"from": 1000,
				"size": 10,
				"query": value.NewValue(map[string]interface{}{
					"match_phrase": "Avengers: Infinity War",
					"field":        "title",
					"analyzer":     "en",
					"boost":        10,
				}),
				"includeLocations": true,
				"fields":           []interface{}{"country", "city"},
			}),
		},
		{
			id: 4,
			query: value.NewValue(map[string]interface{}{
				"from": 1000,
				"size": 10,
				"query": value.NewValue(map[string]interface{}{
					"conjuncts": []interface{}{
						map[string]interface{}{
							"match": "abc",
							"field": "cba",
						},
						map[string]interface{}{
							"match": "xyz",
							"field": "zyx",
						}},
				}),
				"sort": []interface{}{"country, _id, -_score"},
			}),
		},
		{
			id: 5,
			query: value.NewValue(map[string]interface{}{
				"query": value.NewValue(map[string]interface{}{
					"prefix": "Avengers",
					"field":  "title",
				}),
				"sort": []interface{}{"country, _id, -_score"},
			}),
		},
	}

	for i, test := range tests {
		temp, q, err := BuildSearchRequest("", test.query)
		if err != nil {
			t.Fatalf("Expected no error for q: %+v, but got err: %v", test.query, err)
		}
		sr, err := temp.ConvertToBleveSearchRequest()
		if err != nil {
			t.Fatalf("Error in converting to bleve.SearchRequest, err: %v", err)
		}

		switch qq := q.(type) {
		case *query.MatchQuery:
			if qq.Match != "avengers" || qq.FieldVal != "title" || qq.Fuzziness != 2 {
				t.Fatalf("Exception in match query: %v, %v, %v",
					qq.Match, qq.FieldVal, qq.Fuzziness)
			}

			if sr.Size != 10 || sr.From != 0 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

		case *query.PrefixQuery:
			if sr.Size != math.MaxInt64 || sr.From != 0 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}
			if sr.Sort == nil {
				t.Fatalf("incorrect search request formed, with Sort ,"+
					"expected to be nil, got: %v ", sr.Sort)
			}

			sbytes, _ := json.Marshal([]string{"country, _id, -_score"})
			rbytes, _ := json.Marshal(sr.Sort)
			if !reflect.DeepEqual(sbytes, rbytes) {
				t.Fatalf("incorrect search request, expected: %s got: %s", sbytes, rbytes)
			}

		case *query.WildcardQuery:
			if qq.Wildcard != "Avengers*" || qq.FieldVal != "title" {
				t.Fatalf("Exception in wildcard query: %v, %v", qq.Wildcard, qq.FieldVal)
			}

			if sr.Size != 100 || sr.From != 90 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

		case *query.MatchPhraseQuery:
			if qq.MatchPhrase != "Avengers: Infinity War" || qq.FieldVal != "title" ||
				qq.Analyzer != "en" || float64(*qq.BoostVal) != float64(10) {
				t.Fatalf("Exception in match phrase query: %v, %v, %v, %v",
					qq.MatchPhrase, qq.FieldVal, qq.Analyzer, *qq.BoostVal)
			}

			if sr.Size != 10 || sr.From != 1000 {
				t.Fatalf("incorrect search request formed, with size: %v,"+
					" from: %v", sr.Size, sr.From)
			}

			if len(sr.Sort) != 0 {
				t.Fatalf("incorrect search request formed, with Sort ,"+
					"expected to be nil, got: %v ", sr.Sort)
			}

			if !reflect.DeepEqual(sr.Fields, []string{"country", "city"}) {
				t.Fatalf("incorrect search request, fields: %v", sr.Fields)
			}

		case *query.ConjunctionQuery:
			if len(qq.Conjuncts) != 2 {
				t.Fatalf("Exception in conjunction query: %v", len(qq.Conjuncts))
			}
			mq1, ok1 := qq.Conjuncts[0].(*query.MatchQuery)
			mq2, ok2 := qq.Conjuncts[1].(*query.MatchQuery)
			if !ok1 || !ok2 || mq1.Field() != "cba" || mq2.Field() != "zyx" {
				t.Fatalf("Exception in conjunction query")
			}

			sbytes, _ := json.Marshal([]string{"country, _id, -_score"})
			rbytes, _ := json.Marshal(sr.Sort)
			if !reflect.DeepEqual(sbytes, rbytes) {
				t.Fatalf("incorrect search request, expected: %s got: %s", sbytes, rbytes)
			}

		default:
			t.Fatalf("Unexpected query type: %v, for entry: %v", reflect.TypeOf(q), i)
		}
	}
}

func TestBuildProtoSearchRequestWithSortOnScore(t *testing.T) {
	searchInfo := &datastore.FTSSearchInfo{
		Query: value.NewValue(map[string]interface{}{
			"match": "France",
		}),
		Options: nil,
		Order:   []string{"score DESC"},
		Offset:  0,
		Limit:   2,
	}

	sr := &cbft.SearchRequest{
		Q: []byte(`{"match":"France"}`),
	}

	_, err := BuildProtoSearchRequest(sr, searchInfo, nil, datastore.ScanConsistency(""), "", 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildProtoSearchRequestWithATPlus(t *testing.T) {
	searchInfo := &datastore.FTSSearchInfo{
		Query: value.NewValue(map[string]interface{}{
			"match": "test",
		}),
		Options: nil,
		Order:   []string{},
		Offset:  0,
		Limit:   10,
	}

	sr := &cbft.SearchRequest{
		Q: []byte(`{"match":"test"}`),
	}

	// Create a mock vector with entries
	vector := newMockVector([]timestamp.Entry{
		&mockEntry{position: 1, value: 100, guard: "vb1"},
		&mockEntry{position: 2, value: 200, guard: "vb2"},
	})

	searchReq, err := BuildProtoSearchRequest(sr, searchInfo, vector, datastore.AT_PLUS, "test_index", 5000)
	if err != nil {
		t.Fatalf("Expected no error for AT_PLUS, got: %v", err)
	}

	// Verify QueryCtlParams are set
	if len(searchReq.QueryCtlParams) == 0 {
		t.Fatal("QueryCtlParams should be set for AT_PLUS")
	}

	// Unmarshal and verify
	var ctlParams pb.QueryCtlParams
	err = json.Unmarshal(searchReq.QueryCtlParams, &ctlParams)
	if err != nil {
		t.Fatalf("Failed to unmarshal QueryCtlParams: %v", err)
	}

	// Verify timeout is set
	if ctlParams.Ctl == nil || ctlParams.Ctl.Timeout != 5000 {
		t.Fatalf("Expected timeout 5000, got: %v", ctlParams.Ctl.Timeout)
	}

	// Verify consistency level
	if ctlParams.Ctl.Consistency == nil {
		t.Fatal("Consistency params should be set for AT_PLUS")
	}

	if ctlParams.Ctl.Consistency.Level != string(cbgt.ConsistencyLevelAtPlus) {
		t.Fatalf("Expected consistency level %s, got: %s",
			cbgt.ConsistencyLevelAtPlus, ctlParams.Ctl.Consistency.Level)
	}

	// Verify vectors are set
	if ctlParams.Ctl.Consistency.Vectors == nil || len(ctlParams.Ctl.Consistency.Vectors) == 0 {
		t.Fatal("Consistency vectors should be set for AT_PLUS")
	}

	if _, exists := ctlParams.Ctl.Consistency.Vectors["test_index"]; !exists {
		t.Fatal("Consistency vectors should include the index")
	}
}

func TestBuildProtoSearchRequestWithSCANPlus(t *testing.T) {
	searchInfo := &datastore.FTSSearchInfo{
		Query: value.NewValue(map[string]interface{}{
			"match": "test",
		}),
		Options: nil,
		Order:   []string{},
		Offset:  0,
		Limit:   10,
	}

	sr := &cbft.SearchRequest{
		Q: []byte(`{"match":"test"}`),
	}

	// SCAN_PLUS without vectors should work
	searchReq, err := BuildProtoSearchRequest(sr, searchInfo, nil, datastore.SCAN_PLUS, "test_index", 5000)
	if err != nil {
		t.Fatalf("Expected no error for SCAN_PLUS without vectors, got: %v", err)
	}

	// Verify QueryCtlParams are set
	if len(searchReq.QueryCtlParams) == 0 {
		t.Fatal("QueryCtlParams should be set for SCAN_PLUS")
	}

	// Unmarshal and verify
	var ctlParams pb.QueryCtlParams
	err = json.Unmarshal(searchReq.QueryCtlParams, &ctlParams)
	if err != nil {
		t.Fatalf("Failed to unmarshal QueryCtlParams: %v", err)
	}

	// Verify timeout is set
	if ctlParams.Ctl == nil || ctlParams.Ctl.Timeout != 5000 {
		t.Fatalf("Expected timeout 5000, got: %v", ctlParams.Ctl.Timeout)
	}

	// Verify consistency level
	if ctlParams.Ctl.Consistency == nil {
		t.Fatal("Consistency params should be set for SCAN_PLUS")
	}

	if ctlParams.Ctl.Consistency.Level != string(cbgt.ConsistencyLevelScanPlus) {
		t.Fatalf("Expected consistency level %s (scan_plus), got: %s",
			cbgt.ConsistencyLevelScanPlus, ctlParams.Ctl.Consistency.Level)
	}

	// Verify vectors are NOT set for SCAN_PLUS
	if ctlParams.Ctl.Consistency.Vectors != nil && len(ctlParams.Ctl.Consistency.Vectors) > 0 {
		t.Fatal("Consistency vectors should NOT be set for SCAN_PLUS")
	}
}

func TestBuildProtoSearchRequestSCANPlusWithVectorsShouldFail(t *testing.T) {
	searchInfo := &datastore.FTSSearchInfo{
		Query: value.NewValue(map[string]interface{}{
			"match": "test",
		}),
		Options: nil,
		Order:   []string{},
		Offset:  0,
		Limit:   10,
	}

	sr := &cbft.SearchRequest{
		Q: []byte(`{"match":"test"}`),
	}

	// Create a vector with entries
	vector := newMockVector([]timestamp.Entry{
		&mockEntry{position: 1, value: 100, guard: "vb1"},
	})

	// SCAN_PLUS with vectors should return an error
	_, err := BuildProtoSearchRequest(sr, searchInfo, vector, datastore.SCAN_PLUS, "test_index", 5000)
	if err == nil {
		t.Fatal("Expected error when providing vectors with SCAN_PLUS, but got none")
	}

	expectedErrMsg := "consistency vectors are not compatible with SCAN_PLUS consistency level"
	if err.Error() != expectedErrMsg {
		t.Fatalf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestBuildProtoSearchRequestDefaultTimeout(t *testing.T) {
	searchInfo := &datastore.FTSSearchInfo{
		Query: value.NewValue(map[string]interface{}{
			"match": "test",
		}),
		Options: nil,
		Order:   []string{},
		Offset:  0,
		Limit:   10,
	}

	sr := &cbft.SearchRequest{
		Q: []byte(`{"match":"test"}`),
	}

	// Pass timeout of 0, should default to 120000
	searchReq, err := BuildProtoSearchRequest(sr, searchInfo, nil, datastore.ScanConsistency(""), "test_index", 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify QueryCtlParams are set
	if len(searchReq.QueryCtlParams) == 0 {
		t.Fatal("QueryCtlParams should be set even with no consistency")
	}

	// Unmarshal and verify timeout is defaulted
	var ctlParams pb.QueryCtlParams
	err = json.Unmarshal(searchReq.QueryCtlParams, &ctlParams)
	if err != nil {
		t.Fatalf("Failed to unmarshal QueryCtlParams: %v", err)
	}

	if ctlParams.Ctl == nil || ctlParams.Ctl.Timeout != 120000 {
		t.Fatalf("Expected default timeout 120000, got: %v", ctlParams.Ctl.Timeout)
	}

	// Verify no consistency params are set for empty consistency level
	if ctlParams.Ctl.Consistency != nil {
		t.Fatal("Consistency params should NOT be set for empty consistency level")
	}
}
