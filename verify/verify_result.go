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
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/sear"

	"github.com/couchbase/cbft"
	mo "github.com/couchbase/moss"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

func init() {
	registry.RegisterIndexType(sear.Name, sear.New)
}

// NewVerify expects nameAndKeyspace to be either of:
//     - `bucket_name`
//     - `bucket_name.scope_name.collection_name`
func NewVerify(nameAndKeyspace, field string, query, options value.Value,
	parallelism int) (
	datastore.Verify, errors.Error) {
	if query == nil {
		return nil, util.N1QLError(nil, "query/options not provided")
	}

	var skip bool
	if options != nil {
		skipVal, skipValAvailable := options.Field("skipVerify")
		if skipValAvailable {
			if skipVal.Type() == value.BOOLEAN {
				skip = skipVal.Actual().(bool)
			} else if skipVal.Type() == value.STRING {
				if skipVal.Actual().(string) == "true" {
					skip = true
				}
			}
		}
	}

	return &VerifyCtx{
		nameAndKeyspace: nameAndKeyspace,
		field:           field,
		query:           query,
		options:         options,
		skip:            skip,
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

	queryFields, searchRequest, _, err := util.ParseQueryToSearchRequest(
		v.field, v.query)
	if err != nil {
		return util.N1QLError(err, "")
	}

	var idxMapping mapping.IndexMapping
	var scope, collection string
	var typeMappings []string
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

			idxMapping, docConfig, scope, collection, typeMappings, err = util.FetchIndexMapping(
				indexVal.Actual().(string), indexUUID, keyspace)
			if err != nil {
				return util.N1QLError(nil, "index mapping not found")
			}

			if docConfig != nil && strings.HasPrefix(docConfig.Mode, "scope.collection.") {
				idxMapping = OptimizeIndexMapping(idxMapping, scope, collection, queryFields)
			} else {
				idxMapping = OptimizeIndexMapping(idxMapping, "", "", queryFields)
			}
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
	idx, err := bleve.NewUsing("", idxMapping, sear.Name, sear.Name, nil)
	if err != nil {
		return util.N1QLError(err, "")
	}

	defaultType := "_default"
	if imi, ok := idxMapping.(*mapping.IndexMappingImpl); ok {
		defaultType = imi.DefaultType
	}

	v.idx = idx
	v.m = idxMapping
	if v.sr, err = searchRequest.ConvertToBleveSearchRequest(); err != nil {
		return util.N1QLError(err, "could not generate *bleve.SearchRequest")
	}
	v.sr.Size = 1
	v.sr.From = 0
	v.defaultType = defaultType
	v.docConfig = docConfig
	v.scope = scope
	v.collection = collection
	v.typeMappings = typeMappings
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
	skip            bool

	l            sync.RWMutex
	initialised  bool
	m            mapping.IndexMapping
	scope        string
	collection   string
	typeMappings []string
	sr           *bleve.SearchRequest
	defaultType  string
	docConfig    *cbft.BleveDocumentConfig

	idxM sync.Mutex
	idx  bleve.Index
}

func (v *VerifyCtx) Evaluate(item value.Value) (bool, errors.Error) {
	if v.skip {
		// skip evaluation
		return true, nil
	}

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
		bdoc := v.docConfig.BuildDocumentFromObj([]byte(key), doc, v.defaultType)
		if strings.HasPrefix(v.docConfig.Mode, "scope.collection.") &&
			len(v.scope) > 0 && len(v.collection) > 0 {
			// decorate type with scope and collection info
			typ := v.scope + "." + v.collection
			for _, t := range v.typeMappings {
				if bdoc.Type() == t {
					// append type information only if the type mapping specifies a
					// 'type' and the document's matches it.
					typ += "." + t
					break
				}
			}

			bdoc.SetType(typ)
		}
		doc = bdoc
	}

	v.idxM.Lock()
	defer v.idxM.Unlock()

	err := v.idx.Index(key, doc)
	if err != nil {
		return false, util.N1QLError(err, "Index error")
	}

	res, err := v.idx.Search(v.sr)
	if err != nil {
		return false, util.N1QLError(err, "search failed")
	}

	if len(res.Hits) < 1 {
		return false, nil
	}

	return true, nil
}
