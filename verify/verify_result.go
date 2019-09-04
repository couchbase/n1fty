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
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index/store/moss"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/cbft"
	mo "github.com/couchbase/moss"

	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

func init() {
	mo.SkipStats = true
}

func KVConfigForMoss() map[string]interface{} {
	return map[string]interface{}{
		"mossCollectionOptions": map[string]interface{}{
			"MaxPreMergerBatches": math.MaxInt32,
		},
	}
}

func NewVerify(nameAndKeyspace, field string, query, options value.Value) (
	datastore.Verify, errors.Error) {
	if query == nil {
		return nil, util.N1QLError(nil, "query/options not provided")
	}

	return &VerifyCtx{
		nameAndKeyspace: nameAndKeyspace,
		field:           field,
		query:           query,
		options:         options,
	}, nil
}

func (v *VerifyCtx) isCtxInitialised() bool {
	v.l.RLock()
	rv := v.initialised
	v.l.RUnlock()
	return rv
}

func (v *VerifyCtx) initVerifyCtx() errors.Error {
	v.l.Lock()
	if v.initialised {
		v.l.Unlock()
		return nil
	}
	defer v.l.Unlock()

	queryFields, searchRequest, err := util.ParseQueryToSearchRequest(
		v.field, v.query)
	if err != nil {
		return util.N1QLError(err, "")
	}

	var idxMapping mapping.IndexMapping
	var docConfig *cbft.BleveDocumentConfig

	var indexOptionAvailable bool
	if v.options != nil {
		_, indexOptionAvailable = v.options.Field("index")
	}
	if !indexOptionAvailable {
		// in case index option isn't available, use the query fields to
		// build an index mapping that covers all the necessary fields.
		idxMapping = util.BuildIndexMappingOnFields(queryFields, "", "")
	} else {
		indexVal, _ := v.options.Field("index")
		if indexVal.Type() == value.STRING {
			keyspace := util.FetchKeySpace(v.nameAndKeyspace)

			// check if indexUUID string is also available from the options.
			var indexUUID string
			indexUUIDVal, indexUUIDAvailable := v.options.Field("indexUUID")
			if indexUUIDAvailable {
				if indexUUIDVal.Type() == value.STRING {
					indexUUID = indexUUIDVal.Actual().(string)
				}
			}

			idxMapping, docConfig, err = util.FetchIndexMapping(
				indexVal.Actual().(string), indexUUID, keyspace)
			if err != nil {
				return util.N1QLError(nil, "index mapping not found")
			}

			idxMapping = OptimizeIndexMapping(idxMapping, queryFields)
		} else if indexVal.Type() == value.OBJECT {
			idxMapping, _ = util.ConvertValObjectToIndexMapping(indexVal)
			if idxMapping == nil {
				return util.N1QLError(nil, "index object not a valid mapping")
			}
		} else {
			return util.N1QLError(nil, "unrecognizable index option")
		}
	}

	// Set up an in-memory bleve index using moss for evaluating the hits.
	idx, err := bleve.NewUsing("", idxMapping, upsidedown.Name, moss.Name,
		KVConfigForMoss())
	if err != nil {
		return util.N1QLError(err, "")
	}

	// fetch upsidedown & moss collection associated with underlying store
	bleveIndex, kvstore, err := idx.Advanced()
	if err != nil {
		return util.N1QLError(err, "idx.Advanced error")
	}

	udc, ok := bleveIndex.(*upsidedown.UpsideDownCouch)
	if !ok {
		return util.N1QLError(nil, "expected UpsideDownCouch")
	}

	collh, ok := kvstore.(CollectionHolder)
	if !ok {
		return util.N1QLError(nil, "expected kvstore.CollectionHolder")
	}

	defaultType := "_default"
	if imi, ok := idxMapping.(*mapping.IndexMappingImpl); ok {
		defaultType = imi.DefaultType
	}

	v.idx = idx
	v.m = idxMapping
	searchRequest.Size = 1
	searchRequest.From = 0
	v.sr = searchRequest
	v.udc = udc
	v.coll = collh.Collection()
	v.defaultType = defaultType
	v.docConfig = docConfig
	v.initialised = true

	return nil
}

type CollectionHolder interface {
	Collection() mo.Collection
}

type ResetStackDirtyToper interface {
	ResetStackDirtyTop() error
}

type VerifyCtx struct {
	nameAndKeyspace string
	field           string
	query           value.Value
	options         value.Value

	l           sync.RWMutex
	initialised bool
	idx         bleve.Index
	m           mapping.IndexMapping
	sr          *bleve.SearchRequest
	udc         *upsidedown.UpsideDownCouch
	coll        mo.Collection
	defaultType string
	docConfig   *cbft.BleveDocumentConfig
}

func (v *VerifyCtx) Evaluate(item value.Value) (bool, errors.Error) {
	if !v.isCtxInitialised() {
		err := v.initVerifyCtx()
		if err != nil {
			return false, err
		}
	}

	key := "k"
	if annotatedItem, ok := item.(value.AnnotatedValue); ok {
		if keyStr, ok := annotatedItem.GetId().(string); ok {
			key = keyStr
		}
	}

	doc := item.Actual()
	if v.docConfig != nil {
		doc = v.docConfig.BuildDocumentFromObj([]byte(key), doc, v.defaultType)
	}

	kdoc := document.NewDocument(key)

	err := v.m.MapDocument(kdoc, doc)
	if err != nil {
		return false, util.N1QLError(err, "MapDocument error")
	}

	err = v.udc.UpdateWithAnalysis(kdoc, v.udc.Analyze(kdoc), nil)
	if err != nil {
		return false, util.N1QLError(err, "UpdateWithAnalysis error")
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
