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
	"math"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

var bleveMaxResultWindow = int64(10000)

type MappingDetails struct {
	UUID         string
	SourceName   string
	Scope        string
	Collection   string
	IMapping     mapping.IndexMapping
	DocConfig    *cbft.BleveDocumentConfig
	TypeMappings []string
}

var mappingsCacheLock sync.RWMutex
var mappingsCache map[string]map[string]*MappingDetails

var EmptyIndexMapping mapping.IndexMapping

func init() {
	// mappingsCache maps indexname to keyspace to mappingDetails
	mappingsCache = make(map[string]map[string]*MappingDetails)

	EmptyIndexMapping = bleve.NewIndexMapping()
}

func ClearMappingsCache() {
	mappingsCacheLock.Lock()
	mappingsCache = make(map[string]map[string]*MappingDetails)
	mappingsCacheLock.Unlock()
}

func SetIndexMapping(name string, mappingDetails *MappingDetails) {
	if mappingDetails == nil {
		return
	}

	// TODO: do the callers care that they're blowing away any
	// existing mapping?  Consider a race where a slow goroutine
	// incorrectly "wins" by setting an outdated mapping?
	mappingsCacheLock.Lock()
	if _, exists := mappingsCache[name]; !exists {
		mappingsCache[name] = make(map[string]*MappingDetails)
	}

	keyspace := mappingDetails.SourceName
	if mappingDetails.Scope == "_default" && mappingDetails.Collection == "_default" {
		// FTSIndexer1 handles indexes that are mapped to bucket and those
		// mapped to bucket._default._default, so create 2 entries here.
		mappingsCache[name][keyspace] = mappingDetails
		mappingsCache[name][keyspace+"._default._default"] = mappingDetails
	} else if len(mappingDetails.Scope) > 0 && len(mappingDetails.Collection) > 0 {
		keyspace += "." + mappingDetails.Scope + "." + mappingDetails.Collection
		mappingsCache[name][keyspace] = mappingDetails
	} else {
		mappingsCache[name][keyspace] = mappingDetails
	}

	mappingsCacheLock.Unlock()
}

func FetchIndexMapping(name, uuid, keyspace string) (
	mapping.IndexMapping, *cbft.BleveDocumentConfig, string, string, []string, error) {
	if len(keyspace) == 0 || len(name) == 0 {
		// Return default index mapping if keyspace not provided.
		return EmptyIndexMapping, nil, "", "", nil, nil
	}
	mappingsCacheLock.RLock()
	defer mappingsCacheLock.RUnlock()
	if info, exists := mappingsCache[name]; exists {
		if mappingDetails, exists := info[keyspace]; exists {
			if uuid == "" || mappingDetails.UUID == uuid {
				return mappingDetails.IMapping,
					mappingDetails.DocConfig,
					mappingDetails.Scope,
					mappingDetails.Collection,
					mappingDetails.TypeMappings,
					nil
			}
		}
	}
	return nil, nil, "", "", nil, fmt.Errorf("index mapping not found for: %v", name)
}

func BuildIndexMappingOnFields(queryFields map[SearchField]struct{}, defaultAnalyzer string,
	defaultDateTimeParser string) mapping.IndexMapping {
	var build func(field SearchField, m *mapping.DocumentMapping) *mapping.DocumentMapping
	build = func(field SearchField, m *mapping.DocumentMapping) *mapping.DocumentMapping {
		subs := strings.SplitN(field.Name, ".", 2)
		if _, exists := m.Properties[subs[0]]; !exists {
			m.Properties[subs[0]] = &mapping.DocumentMapping{
				Enabled:    true,
				Properties: make(map[string]*mapping.DocumentMapping),
			}
		}

		analyzer := field.Analyzer
		if field.Type == "text" && analyzer == "" {
			analyzer = defaultAnalyzer
		}
		dateFormat := field.DateFormat
		if field.Type == "datetime" && dateFormat == "" {
			dateFormat = defaultDateTimeParser
		}

		if len(subs) == 1 {
			m.Properties[subs[0]].Fields = append(m.Fields, &mapping.FieldMapping{
				Name:               field.Name,
				Type:               field.Type,
				Analyzer:           analyzer,
				DateFormat:         dateFormat,
				Index:              true,
				IncludeTermVectors: true,
			})
		} else {
			// length == 2
			m.Properties[subs[0]] = build(SearchField{
				Name:       subs[1],
				Type:       field.Type,
				Analyzer:   analyzer,
				DateFormat: dateFormat,
			}, m.Properties[subs[0]])
		}

		return m
	}

	idxMapping := bleve.NewIndexMapping()
	docMapping := &mapping.DocumentMapping{
		Enabled:    true,
		Properties: make(map[string]*mapping.DocumentMapping),
	}

	if len(queryFields) == 0 {
		// no fields available, deploy a dynamic default index.
		docMapping.Dynamic = true
	} else {
		for field := range queryFields {
			if len(field.Name) > 0 {
				docMapping = build(field, docMapping)
			} else {
				// in case one of the searcher's field name is not provided,
				// set doc mapping to dynamic and skip processing remaining fields.
				docMapping.Dynamic = true
				docMapping.DefaultAnalyzer = field.Analyzer
				break
			}
		}
	}
	idxMapping.DefaultMapping = docMapping

	return idxMapping
}

func CleanseField(field string) string {
	// The field string provided by N1QL will be enclosed within
	// back-ticks (`) i.e, "`fieldname`". If in case of nested fields
	// it'd look like: "`fieldname`.`nestedfieldname`".
	// To make this searchable, strip the back-ticks from the provided
	// field strings.
	return strings.Replace(field, "`", "", -1)
}

func FetchKeySpace(nameAndKeyspace string) string {
	// Ex: namePlusKeySpace --> keySpace
	// - "`travel`" --> travel
	// - "`default`:`travel`" --> travel
	// - "`default`:`travel`.`scope`.`collection`" --> travel.scope.collection
	if len(nameAndKeyspace) == 0 {
		return ""
	}

	entriesSplitAtColon := strings.Split(nameAndKeyspace, ":")
	keyspace := entriesSplitAtColon[len(entriesSplitAtColon)-1]
	return CleanseField(keyspace)
}

func ParseQueryToSearchRequest(field string, input value.Value) (
	map[SearchField]struct{}, *cbft.SearchRequest, int64, bool, error) {
	field = CleanseField(field)

	queryFields := map[SearchField]struct{}{}
	if input == nil {
		queryFields[SearchField{Name: field}] = struct{}{}
		return queryFields, nil, 0, false, nil
	}

	var err error
	var q query.Query

	rv := &cbft.SearchRequest{}
	var ctlTimeout int64

	// if the input has a query field that is an object type
	// then it is a search request
	if qf, ok := input.Field("query"); ok && qf.Type() == value.OBJECT {
		rv, q, err = BuildSearchRequest(field, input)
		if err != nil {
			return nil, nil, 0, false, err
		}

		if cf, ok := input.Field("ctl"); ok && cf.Type() == value.OBJECT {
			if tf, ok := cf.Field("timeout"); ok && tf.Type() == value.NUMBER {
				ctlTimeout = int64(tf.Actual().(float64))
			}
		}
	} else {
		q, err = BuildQuery(field, input)
		if err != nil {
			return nil, nil, 0, false, err
		}
		rv.Q, err = json.Marshal(q)
		if err != nil {
			return nil, nil, 0, false, err
		}

		size := math.MaxInt64
		rv.Size = &size

		rv.Sort = nil
	}

	queryFields, err = FetchFieldsToSearchFromQuery(q)
	if err != nil {
		return nil, nil, 0, false, err
	}

	needsFiltering := queryResultsNeedFiltering(q)

	return queryFields, rv, ctlTimeout, needsFiltering, nil
}

func queryResultsNeedFiltering(q query.Query) bool {
	if _, ok := q.(*query.MatchAllQuery); ok {
		// A match_all query would obtain all document IDs that
		// KV shipped to FTS and not necessarily the ones that
		// were indexed.
		return true
	}

	if bq, ok := q.(*query.BooleanQuery); ok {
		if bq.MustNot != nil {
			// A NEGATE search would obtain all document IDs that
			// KV shipped to FTS that did not match the search
			// criteria.
			if dq, ok := bq.MustNot.(*query.DisjunctionQuery); ok {
				if len(dq.Disjuncts) > 0 {
					return true
				}
			}
		}
	}

	if qsq, ok := q.(*query.QueryStringQuery); ok {
		if qq, err := qsq.Parse(); err == nil {
			if bq, ok := qq.(*query.BooleanQuery); ok {
				if bq.MustNot != nil {
					// A NEGATE search would obtain all document IDs that
					// KV shipped to FTS that did not match the search
					// criteria.
					if dq, ok := bq.MustNot.(*query.DisjunctionQuery); ok {
						if len(dq.Disjuncts) > 0 {
							return true
						}
					}

				}
			}
		}
	}

	return false
}

// Value MUST be an object
func ConvertValObjectToIndexMapping(val value.Value) (
	im *mapping.IndexMappingImpl, err error) {
	// TODO: seems inefficient to hop to JSON and back?
	valBytes, err := val.MarshalJSON()
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(valBytes, &im)
	return im, err
}

func N1QLError(err error, desc string) errors.Error {
	return errors.NewError(err, "n1fty: "+desc)
}

func GetBleveMaxResultWindow() int64 {
	return atomic.LoadInt64(&bleveMaxResultWindow)
}

func SetBleveMaxResultWindow(v int64) {
	atomic.StoreInt64(&bleveMaxResultWindow, v)
}
