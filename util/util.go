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
	"strings"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/couchbase/query/value"
)

type MappingDetails struct {
	UUID       string
	SourceName string
	IMapping   mapping.IndexMapping
}

var mappingsCacheLock sync.RWMutex
var mappingsCache map[string]*MappingDetails

func init() {
	mappingsCache = make(map[string]*MappingDetails)
}

func SetIndexMapping(name string, mappingDetails *MappingDetails) {
	mappingsCacheLock.Lock()
	mappingsCache[name] = mappingDetails
	mappingsCacheLock.Unlock()
}

func FetchIndexMapping(name, keyspace string) (mapping.IndexMapping, error) {
	if len(keyspace) == 0 || len(name) == 0 {
		// Return default index mapping if keyspace not provided.
		return NewIndexMappingWithAnalyzer(""), nil
	}
	mappingsCacheLock.RLock()
	defer mappingsCacheLock.RUnlock()
	if info, exists := mappingsCache[name]; exists {
		if info.SourceName == keyspace {
			return info.IMapping, nil
		}
	}
	return nil, fmt.Errorf("index mapping not found for: %v", name)
}

func NewIndexMappingWithAnalyzer(analyzer string) mapping.IndexMapping {
	idxMapping := bleve.NewIndexMapping()
	if analyzer != "" {
		idxMapping.DefaultAnalyzer = analyzer
	}

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

func FetchQueryFields(field string, query value.Value) ([]SearchField, []byte, error) {
	var queryFields []SearchField
	var qBytes []byte
	var err error

	field = CleanseField(field)
	if query != nil {
		qBytes, err = BuildQueryBytes(field, query)
		if err != nil {
			return nil, nil, err
		}

		queryFields, err = FetchFieldsToSearchFromQuery(qBytes)
		if err != nil {
			return nil, nil, err
		}
	} else {
		queryFields = []SearchField{{
			Name: field,
		}}
	}

	return queryFields, qBytes, err
}

// Value MUST be an object
func ConvertValObjectToIndexMapping(val value.Value) (mapping.IndexMapping, error) {
	valBytes, err := val.MarshalJSON()
	if err != nil {
		return nil, err
	}

	var im *mapping.IndexMappingImpl
	err = json.Unmarshal(valBytes, &im)
	return im, err
}
