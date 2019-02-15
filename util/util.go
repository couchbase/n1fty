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
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/mapping"
)

var indexMappingsLock sync.RWMutex
var indexMappings map[string]mapping.IndexMapping

func init() {
	indexMappings = make(map[string]mapping.IndexMapping)
}

func SetIndexMapping(name string, iMapping mapping.IndexMapping) {
	indexMappingsLock.Lock()
	indexMappings[name] = iMapping
	indexMappingsLock.Unlock()
}

func FetchIndexMapping(name string) (mapping.IndexMapping, error) {
	indexMappingsLock.RLock()
	defer indexMappingsLock.RUnlock()
	if iMapping, exists := indexMappings[name]; exists {
		return iMapping, nil
	}
	return nil, fmt.Errorf("index mapping not found for: %v", name)
}

func CleanseField(field string) string {
	// The field string provided by N1QL will be enclosed within
	// back-ticks (`) i.e, "`fieldname`". If in case of nested fields
	// it'd look like: "`fieldname`.`nestedfieldname`".
	// To make this searchable, strip the back-ticks from the provided
	// field strings.
	return strings.Replace(field, "`", "", -1)
}
