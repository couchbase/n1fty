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

package util

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

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

func BuildQueryFromSearchRequestBytes(field string, sBytes []byte) (query.Query, error) {
	sr := bleve.SearchRequest{}
	err := json.Unmarshal(sBytes, &sr)
	if err != nil {
		return nil, err
	}

	if field != "" {
		UpdateFieldsInQuery(sr.Query, field)
	}

	return sr.Query, nil
}

func BuildSearchRequest(field string, input value.Value) (*pb.SearchRequest,
	query.Query, error) {
	if input == nil {
		return nil, nil, fmt.Errorf("query not provided")
	}

	srBytes, err := input.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}

	sr := &bleve.SearchRequest{}
	err = json.Unmarshal(srBytes, &sr)
	if err != nil {
		return nil, nil, err
	}

	if field != "" {
		UpdateFieldsInQuery(sr.Query, field)
	}

	rv := &pb.SearchRequest{}
	rv.Contents, err = json.Marshal(sr)
	if err != nil {
		return rv, sr.Query, err
	}

	return rv, sr.Query, nil
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

func BuildSortFromBytes(sBytes []byte) (search.SortOrder, error) {
	if sBytes == nil {
		return nil, nil
	}
	return search.ParseSortOrderJSON(append([]json.RawMessage(nil), sBytes))
}

func ParseSearchInfoToSearchRequest(searchRequest **pb.SearchRequest,
	searchInfo *datastore.FTSSearchInfo, vector timestamp.Vector,
	consistencyLevel datastore.ScanConsistency, indexName string, isComplete bool) error {
	sr := &bleve.SearchRequest{}
	err := json.Unmarshal((*searchRequest).Contents, sr)
	if err != nil {
		return err
	}
	(*searchRequest).IndexName = indexName

	// need to complete the searchrequest from SearchInfo details
	if !isComplete {
		sr.From = int(searchInfo.Offset)
		if sr.Size == 0 {
			sr.Size = int(searchInfo.Limit)
		}

		// searchInfo order takes precedence
		if len(searchInfo.Order) > 0 {
			var tempOrder []string
			for _, so := range searchInfo.Order {
				fields := strings.Fields(so)
				field := fields[0]
				if field == "score" || field == "id" {
					field = "_" + field
				}

				if len(fields) == 1 || (len(fields) == 2 &&
					fields[1] == "ASC") {
					tempOrder = append(tempOrder, field)
					continue
				}

				tempOrder = append(tempOrder, "-"+field)
			}

			sr.Sort = search.ParseSortOrderStrings(tempOrder)
		}
	}

	// check whether the streaming of results is beneficial
	if (sr.Sort == nil && len(searchInfo.Order) == 0) ||
		sr.Size+sr.From > int(GetBleveMaxResultWindow()) {
		(*searchRequest).Stream = true
	}

	(*searchRequest).Contents, err = json.Marshal(sr)
	if err != nil {
		return err
	}

	if vector != nil && len(vector.Entries()) > 0 {
		ctlParams := &pb.QueryCtlParams{
			Ctl: &pb.QueryCtl{
				Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
				Consistency: &pb.ConsistencyParams{
					Vectors: make(map[string]*pb.ConsistencyVectors, 1),
				},
			},
		}

		if consistencyLevel == datastore.SCAN_PLUS {
			ctlParams.Ctl.Consistency.Level = "at_plus"
		}

		vMap := &pb.ConsistencyVectors{
			ConsistencyVector: make(map[string]uint64, 1024),
		}

		for _, entry := range vector.Entries() {
			key := strconv.FormatInt(int64(entry.Position()), 10) + "/" + entry.Guard()
			vMap.ConsistencyVector[key] = uint64(entry.Value())
		}

		ctlParams.Ctl.Consistency.Vectors[indexName] = vMap

		(*searchRequest).QueryCtlParams, err = json.Marshal(ctlParams)
		if err != nil {
			return err
		}
	}

	return nil
}
