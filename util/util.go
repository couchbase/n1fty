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

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/value"
)

var bleveMaxResultWindow = int64(10000)

type MappingDetails struct {
	UUID       string
	SourceName string
	IMapping   mapping.IndexMapping
	DocConfig  *cbft.BleveDocumentConfig
}

var mappingsCacheLock sync.RWMutex
var mappingsCache map[string]*MappingDetails

var EmptyIndexMapping mapping.IndexMapping

func init() {
	mappingsCache = make(map[string]*MappingDetails)

	EmptyIndexMapping = bleve.NewIndexMapping()
}

func SetIndexMapping(name string, mappingDetails *MappingDetails) {
	// TODO: do the callers care that they're blowing away any
	// existing mapping?  Consider a race where a slow goroutine
	// incorrectly "wins" by setting an outdated mapping?
	mappingsCacheLock.Lock()
	mappingsCache[name] = mappingDetails
	mappingsCacheLock.Unlock()
}

func FetchIndexMapping(name, uuid, keyspace string) (mapping.IndexMapping, *cbft.BleveDocumentConfig, error) {
	if len(keyspace) == 0 || len(name) == 0 {
		// Return default index mapping if keyspace not provided.
		return EmptyIndexMapping, nil, nil
	}
	mappingsCacheLock.RLock()
	defer mappingsCacheLock.RUnlock()
	if info, exists := mappingsCache[name]; exists {
		// validate sourceName/keyspace, additionally check UUID if provided
		if info.SourceName == keyspace {
			if uuid == "" || info.UUID == uuid {
				return info.IMapping, info.DocConfig, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("index mapping not found for: %v", name)
}

func BuildIndexMappingOnFields(fields []SearchField, defaultAnalyzer string,
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

	if len(fields) == 0 {
		// no fields available, deploy a dynamic default index.
		docMapping.Dynamic = true
	} else {
		for _, field := range fields {
			if len(field.Name) > 0 {
				docMapping = build(field, docMapping)
			} else {
				// in case one of the searcher's field name is not provided,
				// set doc mapping to dynamic and skip processing remaining fields.
				docMapping.Dynamic = true
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
	[]SearchField, *pb.SearchRequest, error) {
	field = CleanseField(field)

	if input == nil {
		return []SearchField{{Name: field}}, nil, nil
	}

	var err error
	var query query.Query

	rv := &pb.SearchRequest{}
	// if the input has a query field that is an object type
	// then it is a search request
	if qf, ok := input.Field("query"); ok && qf.Type() == value.OBJECT {
		rv, query, err = BuildSearchRequest(field, input)
		if err != nil {
			return nil, nil, err
		}
	} else {
		query, err = BuildQuery(field, input)
		if err != nil {
			return nil, nil, err
		}
		sr := bleve.SearchRequest{Query: query,
			From: 0,
			Size: math.MaxInt64,
			Sort: nil}
		rv.Contents, err = json.Marshal(sr)
		if err != nil {
			return nil, nil, err
		}
	}

	queryFields, err := FetchFieldsToSearchFromQuery(query)
	if err != nil {
		return nil, nil, err
	}

	return queryFields, rv, nil
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
