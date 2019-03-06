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

	"github.com/blevesearch/bleve/search/query"
	pb "github.com/couchbase/cbft/protobuf"
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

func BuildSearchRequest(field string, input value.Value) (*pb.SearchRequest,
	query.Query, error) {
	if input == nil {
		return nil, nil, fmt.Errorf("query not provided")
	}

	// take the query object to parse and update fields
	var err error
	var qBytes []byte
	if tq, ok := input.Field("query"); ok {
		qBytes, err = tq.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}
	}

	// resetting the query object as to avoid unmarshal
	// conflicts later wrt to mismatched types on
	// pb.SearchRequest.Query ([]byte vs object)
	input.SetField("query", nil)

	// needs a better way to handle both query and sort fields here??
	var sortBytes []byte
	if sq, ok := input.Field("Sort"); ok {
		sortBytes, err = sq.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}
		input.SetField("Sort", nil)
	}

	srBytes, err := input.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}

	rv := &pb.SearchRequest{}
	err = json.Unmarshal(srBytes, &rv)
	if err != nil {
		return nil, nil, err
	}

	// set the sort bytes
	rv.Sort = sortBytes

	// get the query
	q, err := BuildQueryFromBytes(field, qBytes)
	if err != nil {
		return nil, nil, err
	}

	if field != "" {
		UpdateFieldsInQuery(q, field)
	}

	rv.Query, err = json.Marshal(q)
	if err != nil {
		return nil, nil, err
	}

	return rv, q, nil
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
