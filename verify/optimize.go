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

	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/n1fty/util"
)

// OptimizeIndexMapping returns an index mapping that's focused only
// on the given search fields, so that Verify.Evaluate() does not need
// to wastefully index fields that aren't being searched.
func OptimizeIndexMapping(idxMapping mapping.IndexMapping,
	searchFields []util.SearchField) mapping.IndexMapping {
	im, ok := idxMapping.(*mapping.IndexMappingImpl)
	if !ok {
		return idxMapping
	}

	searchFieldsMap := map[util.SearchField]struct{}{}

	for _, sf := range searchFields {
		if sf.Name == "" {
			// For now, early return if "_all" field is searched.
			return idxMapping
		}

		searchFieldsMap[sf] = struct{}{}
	}

	rv := *im // Shallow copy.

	// Optimization when default dynamic type mapping is enabled,
	// build index mapping based on search fields;
	// Return right away.
	if im.DefaultMapping.Enabled && im.DefaultMapping.Dynamic {
		defaultAnalyzer := im.DefaultMapping.DefaultAnalyzer
		if defaultAnalyzer == "" {
			defaultAnalyzer = im.DefaultAnalyzer
		}
		return util.BuildIndexMappingOnFields(searchFields,
			defaultAnalyzer, im.DefaultDateTimeParser)
	}

	rv.TypeMapping = nil

	for t, dm := range im.TypeMapping {
		// Optimization when any of the top-level mappings is dynamic
		// and enabled, build index mapping based on search fields;
		// Return right away.
		if dm.Enabled && dm.Dynamic {
			defaultAnalyzer := dm.DefaultAnalyzer
			if defaultAnalyzer == "" {
				defaultAnalyzer = im.DefaultAnalyzer
			}
			return util.BuildIndexMappingOnFields(searchFields,
				defaultAnalyzer, im.DefaultDateTimeParser)
		}

		dmOptimized := optimizeDocumentMapping(searchFieldsMap,
			nil, dm, im.DefaultAnalyzer, im.DefaultDateTimeParser)
		if dmOptimized != nil &&
			(dmOptimized.Dynamic ||
				len(dmOptimized.Fields) > 0 ||
				len(dmOptimized.Properties) > 0) {
			if rv.TypeMapping == nil {
				rv.TypeMapping = map[string]*mapping.DocumentMapping{}
			}

			rv.TypeMapping[t] = dmOptimized
		}
	}

	rv.DefaultMapping = optimizeDocumentMapping(searchFieldsMap,
		nil, im.DefaultMapping, im.DefaultAnalyzer, im.DefaultDateTimeParser)

	return &rv
}

func optimizeDocumentMapping(searchFieldsMap map[util.SearchField]struct{},
	path []string, dm *mapping.DocumentMapping, defaultAnalyzer string,
	defaultDateTimeParser string) *mapping.DocumentMapping {
	// TODO: One day optimize dynamic with more granularity.
	if dm == nil || !dm.Enabled || dm.Dynamic {
		return dm
	}

	if dm.DefaultAnalyzer != "" {
		defaultAnalyzer = dm.DefaultAnalyzer
	}

	rv := *dm // Shallow copy.
	rv.Properties = nil
	rv.Fields = nil

	if len(path) > 0 {
		for _, f := range dm.Fields {
			if f.Index {
				fpath := append([]string(nil), path...) // Copy.
				fpath[len(fpath)-1] = f.Name

				var analyzer string
				var dateFormat string
				if f.Type == "text" {
					analyzer = f.Analyzer
					if f.Analyzer == "" {
						analyzer = defaultAnalyzer
					}
				} else if f.Type == "datetime" {
					dateFormat = f.DateFormat
					if f.DateFormat == "" {
						dateFormat = defaultDateTimeParser
					}
				}

				ftrack := strings.Join(fpath, ".")

				_, exists := searchFieldsMap[util.SearchField{
					Name:       ftrack,
					Type:       f.Type,
					Analyzer:   analyzer,
					DateFormat: dateFormat,
				}]
				if !exists {
					_, exists = searchFieldsMap[util.SearchField{
						Name: ftrack,
						Type: f.Type,
					}]
				}
				if exists {
					rv.Fields = append(rv.Fields, f)
				}
			}
		}
	}

	for propName, propDM := range dm.Properties {
		propDMOptimized := optimizeDocumentMapping(searchFieldsMap,
			append(path, propName), propDM, defaultAnalyzer, defaultDateTimeParser)
		if propDMOptimized != nil &&
			(propDMOptimized.Dynamic ||
				len(propDMOptimized.Fields) > 0 ||
				len(propDMOptimized.Properties) > 0) {
			if rv.Properties == nil {
				rv.Properties = map[string]*mapping.DocumentMapping{}
			}

			rv.Properties[propName] = propDMOptimized
		}
	}

	return &rv
}
