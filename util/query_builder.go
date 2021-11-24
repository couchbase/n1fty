// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package util

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

func unmarshalSearchRequest(field string, input []byte) (
	*cbft.SearchRequest, query.Query, error) {
	var sr *cbft.SearchRequest
	err := json.Unmarshal(input, &sr)
	if err != nil {
		return nil, nil, err
	}

	temp, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		return nil, nil, err
	}

	// if both size and limit were missing, set the request size to MaxInt64,
	// so as to stream results
	if sr.Size == nil && sr.Limit == nil {
		size := math.MaxInt64
		sr.Size = &size
	}

	// if sort were nil, set request's sort to nil.
	if sr.Sort == nil {
		sr.Sort = []json.RawMessage{}
	}

	if field != "" {
		UpdateFieldsInQuery(temp.Query, field)
		if sr.Q, err = json.Marshal(temp.Query); err != nil {
			return nil, nil, err
		}
	}

	return sr, temp.Query, nil
}

func UpdateFieldsInQuery(q query.Query, field string) {
	switch que := q.(type) {
	case *query.BooleanQuery:
		UpdateFieldsInQuery(que.Must, field)
		UpdateFieldsInQuery(que.Should, field)
		UpdateFieldsInQuery(que.MustNot, field)
	case *query.ConjunctionQuery:
		for i := 0; i < len(que.Conjuncts); i++ {
			UpdateFieldsInQuery(que.Conjuncts[i], field)
		}
	case *query.DisjunctionQuery:
		for i := 0; i < len(que.Disjuncts); i++ {
			UpdateFieldsInQuery(que.Disjuncts[i], field)
		}
	default:
		if fq, ok := que.(query.FieldableQuery); ok {
			if fq.Field() == "" {
				fq.SetField(field)
			}
		}
	}
}

// -----------------------------------------------------------------------------

func BuildQuery(field string, input value.Value) (q query.Query, err error) {
	if input == nil {
		return nil, fmt.Errorf("query not provided")
	}

	if input.Type() == value.STRING {
		return BuildQueryFromString(field, input.Actual().(string))
	}

	if input.Type() == value.OBJECT {
		qBytes, err := input.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return BuildQueryFromBytes(field, qBytes)
	}

	return nil, fmt.Errorf("unsupported query type: %v", input.Type().String())
}

func BuildQueryFromBytes(field string, qBytes []byte) (query.Query, error) {
	q, err := query.ParseQuery(qBytes)
	if err != nil {
		return nil, fmt.Errorf("BuildQueryFromBytes, err: %v", err)
	}

	if field != "" {
		UpdateFieldsInQuery(q, field)
	}

	return q, nil
}

func BuildQueryFromSearchRequest(field string,
	sr *bleve.SearchRequest) (query.Query, error) {
	if field != "" {
		UpdateFieldsInQuery(sr.Query, field)
	}

	return sr.Query, nil
}

func BuildSearchRequest(field string, input value.Value) (*cbft.SearchRequest,
	query.Query, error) {
	if input == nil {
		return nil, nil, fmt.Errorf("query not provided")
	}

	srBytes, err := input.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}

	sr, q, err := unmarshalSearchRequest(field, srBytes)
	if err != nil {
		return nil, nil, err
	}

	return sr, q, nil
}

func BuildQueryFromString(field, input string) (query.Query, error) {
	qsq := query.NewQueryStringQuery(input)

	q, err := qsq.Parse()
	if err != nil {
		return nil, fmt.Errorf("BuildQueryFromString, err: %v", err)
	}

	if field != "" {
		UpdateFieldsInQuery(q, field)
	}

	return q, nil
}

// CheckForPagination looks for any of the pagination
// details in the given search request
func CheckForPagination(input value.Value) bool {
	if input == nil {
		return false
	}

	srBytes, err := input.MarshalJSON()
	if err != nil {
		return false
	}

	sr, _, err := unmarshalSearchRequest("", srBytes)
	if err != nil {
		return false
	}

	// if any of them is set, then pagination is found.
	if (sr.Size != nil && *(sr.Size) >= 0 && *(sr.Size) != math.MaxInt64) ||
		(sr.From != nil && *(sr.From) > 0) ||
		len(sr.Sort) > 0 {
		return true
	}

	return false
}

func BuildProtoSearchRequest(sr *cbft.SearchRequest,
	searchInfo *datastore.FTSSearchInfo, vector timestamp.Vector,
	consistencyLevel datastore.ScanConsistency,
	indexName string) (*pb.SearchRequest, error) {
	searchRequest := &pb.SearchRequest{
		IndexName: indexName,
	}

	// if original request was of query form then, override with
	// searchInfo order details
	if sr.Sort == nil && len(searchInfo.Order) > 0 {
		var tempOrder []string
		for _, so := range searchInfo.Order {
			fields := strings.Fields(so)
			field := fields[0]
			if field == "score" || field == "id" {
				field = "_" + field
			}

			if len(fields) == 1 || (len(fields) == 2 &&
				fields[1] == "ASC") {
				tempOrder = append(tempOrder, `"`+field+`"`)
				continue
			}

			tempOrder = append(tempOrder, `"-`+field+`"`)
		}

		sr.Sort = make([]json.RawMessage, len(tempOrder))
		for i := range tempOrder {
			if err := sr.Sort[i].UnmarshalJSON([]byte(tempOrder[i])); err != nil {
				return nil, err
			}
		}
	}

	// Stream results when ..
	// - SearchRequest: Sort method NOT provided
	// - SearchRequest: From + Size exceeds window

	if sr.From == nil || *(sr.From) < 0 {
		from := int(searchInfo.Offset)
		sr.From = &from
	}

	if sr.Size == nil || *(sr.Size) < 0 || *(sr.Size) == math.MaxInt64 {
		if int(searchInfo.Limit) != math.MaxInt64 {
			limit := int(searchInfo.Limit)
			sr.Size = &limit
		} else {
			size := 0
			sr.Size = &size
			searchRequest.Stream = true
		}
	}

	if sr.Sort == nil && len(searchInfo.Order) == 0 {
		searchRequest.Stream = true
	}

	if (*(sr.Size) + *(sr.From)) > int(GetBleveMaxResultWindow()) {
		searchRequest.Stream = true
		zero := 0
		sr.From = &zero
		sr.Size = &zero
	}

	var err error
	searchRequest.Contents, err = json.Marshal(sr)
	if err != nil {
		return nil, err
	}

	if consistencyLevel == datastore.AT_PLUS &&
		vector != nil && len(vector.Entries()) > 0 {
		ctlParams := &pb.QueryCtlParams{
			Ctl: &pb.QueryCtl{
				Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
				Consistency: &pb.ConsistencyParams{
					Level:   "at_plus",
					Vectors: make(map[string]*pb.ConsistencyVectors, 1),
				},
			},
		}

		vMap := &pb.ConsistencyVectors{
			ConsistencyVector: make(map[string]uint64, 1024),
		}

		for _, entry := range vector.Entries() {
			key := strconv.FormatInt(int64(entry.Position()), 10) + "/" + entry.Guard()
			vMap.ConsistencyVector[key] = uint64(entry.Value())
		}

		ctlParams.Ctl.Consistency.Vectors[indexName] = vMap

		searchRequest.QueryCtlParams, err = json.Marshal(ctlParams)
		if err != nil {
			return nil, err
		}
	}

	return searchRequest, nil
}

// Sets collection information within the provided SearchRequest
func DecorateSearchRequest(sr *cbft.SearchRequest, collection string) *cbft.SearchRequest {
	if sr == nil || len(collection) == 0 {
		return sr
	}

	sr.Collections = []string{collection}
	return sr
}
