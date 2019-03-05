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
	"math"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/store/moss"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"

	mo "github.com/couchbase/moss"

	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

var kvConfigMoss = map[string]interface{}{
	"mossCollectionOptions": map[string]interface{}{
		"MaxPreMergerBatches": math.MaxInt32,
	},
}

func init() {
	mo.SkipStats = true
}

func NewVerify(nameAndKeyspace, field string, query, options value.Value) (
	datastore.Verify, errors.Error) {
	if query == nil {
		return nil, util.N1QLError(nil, "query/options not provided")
	}

	queryFields, qBytes, err := util.FetchQueryFields(field, query)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	var idxMapping mapping.IndexMapping

	var indexOptionAvailable bool
	if options != nil {
		_, indexOptionAvailable = options.Field("index")
	}

	keyspace := util.FetchKeySpace(nameAndKeyspace)

	if !indexOptionAvailable {
		// in case index option not available, use any query field's analyzer,
		// as we'd have reached this point only if all the query fields shared
		// the same analyzer, if query fields unavailable use "standard".
		var analyzer string
		if len(queryFields) > 0 {
			analyzer = queryFields[0].Analyzer
		}
		idxMapping = util.NewIndexMappingWithAnalyzer(analyzer)
	} else {
		indexVal, _ := options.Field("index")
		if indexVal.Type() == value.STRING {
			idxMapping, err = util.FetchIndexMapping(
				indexVal.Actual().(string), keyspace)
			if err != nil {
				return nil, util.N1QLError(nil, "index mapping not found")
			}
		} else if indexVal.Type() == value.OBJECT {
			idxMapping, _ = util.ConvertValObjectToIndexMapping(indexVal)
			if idxMapping == nil {
				return nil, util.N1QLError(nil, "index object not a valid mapping")
			}
		} else {
			return nil, util.N1QLError(nil, "unrecognizable index option")
		}
	}

	q, err := util.BuildQueryFromBytes(field, qBytes)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	// Set up an in-memory bleve index using moss for evaluating
	// the hits.
	idx, err := bleve.NewUsing("", idxMapping, upsidedown.Name, moss.Name, kvConfigMoss)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	// fetch moss collection associated with the underlying store
	var coll mo.Collection
	_, kvstore, err := idx.Advanced()
	if err == nil {
		collh, ok := kvstore.(CollectionHolder)
		if ok {
			coll = collh.Collection()
		}
	}

	return &VerifyCtx{
		idx:  idx,
		sr:   bleve.NewSearchRequest(q),
		coll: coll,
	}, nil
}

type CollectionHolder interface {
	Collection() mo.Collection
}

type ResetStackDirtyToper interface {
	ResetStackDirtyTop() error
}

type VerifyCtx struct {
	idx  bleve.Index
	sr   *bleve.SearchRequest
	coll mo.Collection
}

func (v *VerifyCtx) Evaluate(item value.Value) (bool, errors.Error) {
	err := v.idx.Index("k", item.Actual())
	if err != nil {
		return false, util.N1QLError(err, "could not insert doc into index")
	}

	res, err := v.idx.Search(v.sr)
	if err != nil {
		return false, util.N1QLError(err, "search failed")
	}

	if rsdt, ok := v.coll.(ResetStackDirtyToper); ok && rsdt != nil {
		rsdt.ResetStackDirtyTop()
	}

	if len(res.Hits) < 1 {
		return false, nil
	}

	return true, nil
}
