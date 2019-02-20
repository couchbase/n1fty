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
	"github.com/couchbase/query/value"
)

func updateFieldsInQuery(q query.Query, field string) {
	switch que := q.(type) {
	case *query.BooleanQuery:
		updateFieldsInQuery(que.Must, field)
		updateFieldsInQuery(que.Should, field)
		updateFieldsInQuery(que.MustNot, field)
	case *query.ConjunctionQuery:
		for i := 0; i < len(que.Conjuncts); i++ {
			updateFieldsInQuery(que.Conjuncts[i], field)
		}
	case *query.DisjunctionQuery:
		for i := 0; i < len(que.Disjuncts); i++ {
			updateFieldsInQuery(que.Disjuncts[i], field)
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

func BuildQuery(field string, input value.Value) (query.Query, error) {
	qBytes, err := BuildQueryBytes(field, input)
	if err != nil {
		return nil, fmt.Errorf("BuildQuery err: %v", err)
	}

	return query.ParseQuery(qBytes)
}

func BuildQueryBytes(field string, input value.Value) ([]byte, error) {
	if input == nil {
		return nil, fmt.Errorf("query not provided")
	}

	if input.Type() == value.STRING {
		return buildQueryFromString(field, input.Actual().(string))
	} else if input.Type() == value.OBJECT {
		return input.MarshalJSON()
	}

	return nil, fmt.Errorf("unsupported query type: %v", input.Type().String())
}

func buildQueryFromString(field, input string) ([]byte, error) {
	qsq := query.NewQueryStringQuery(input)
	q, err := qsq.Parse()
	if err != nil {
		return nil, fmt.Errorf("BuildQueryBytes Parse, err: %v", err)
	}

	if field != "" {
		updateFieldsInQuery(q, field)
	}

	return json.Marshal(q)
}
