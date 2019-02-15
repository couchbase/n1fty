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

package verify

import (
	"github.com/blevesearch/bleve"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

func NewVerify(keyspace, field string, query, options value.Value) (
	datastore.Verify, errors.Error) {
	field = util.CleanseField(field)

	if query == nil {
		return nil, errors.NewError(nil, "query not provided")
	}

	// TODO: fetch index name/mapping here as well, to apply on the
	// in-memory index that we're about to build.

	q, err := util.BuildQuery(field, query, options)
	if err != nil {
		return nil, errors.NewError(err, "")
	}

	// TODO: use custom mapping, if available/provided
	idxMapping := bleve.NewIndexMapping()

	idx, err := bleve.NewMemOnly(idxMapping)
	if err != nil {
		return nil, errors.NewError(err, "")
	}

	return &VerifyCtx{
		idx: idx,
		sr:  bleve.NewSearchRequest(q),
	}, nil
}

type VerifyCtx struct {
	idx bleve.Index
	sr  *bleve.SearchRequest
}

func (v *VerifyCtx) Evaluate(item value.Value) (bool, errors.Error) {
	err := v.idx.Index("temp_doc", item.Actual())
	if err != nil {
		return false, errors.NewError(err, "could not insert doc into index")
	}

	defer func() {
		v.idx.Delete("temp_doc")
	}()

	res, err := v.idx.Search(v.sr)
	if err != nil {
		return false, errors.NewError(err, "search failed")
	}

	if len(res.Hits) < 1 {
		return false, nil
	}

	return true, nil
}
