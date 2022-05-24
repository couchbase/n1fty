// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package verify

import (
	"strings"

	"github.com/blevesearch/bleve/v2/mapping"

	"github.com/couchbase/n1fty/util"
)

// OptimizeIndexMapping returns an index mapping that's focused only
// on the given search fields, so that Verify.Evaluate() does not need
// to wastefully index fields that aren't being searched.
func OptimizeIndexMapping(idxMapping mapping.IndexMapping,
	scope, collection string,
	queryFields map[util.SearchField]struct{}) mapping.IndexMapping {
	im, ok := idxMapping.(*mapping.IndexMappingImpl)
	if !ok {
		return idxMapping
	}

	for sf := range queryFields {
		if sf.Name == "" {
			// For now, early return if "_all" field is searched.
			return idxMapping
		}
	}

	rv := *im // Shallow copy.

	// Optimization when default dynamic type mapping is enabled,
	// build index mapping based on search fields if and only if
	// there's no sub fields or sub properties defined.
	// Return right away.
	if im.DefaultMapping.Enabled && im.DefaultMapping.Dynamic &&
		len(im.DefaultMapping.Fields) == 0 && len(im.DefaultMapping.Properties) == 0 {
		defaultAnalyzer := im.DefaultMapping.DefaultAnalyzer
		if defaultAnalyzer == "" {
			defaultAnalyzer = im.DefaultAnalyzer
		}
		optimizedMapping := util.BuildIndexMappingOnFields(queryFields,
			defaultAnalyzer, im.DefaultDateTimeParser)
		// MB-52263
		// Update only default mapping to optimized definition's default mapping;
		// This is to retain any custom analysis components' definitions.
		rv.DefaultMapping = optimizedMapping.DefaultMapping
		return &rv
	}

	rv.TypeMapping = nil

	var scopeCollPrefix string
	if len(scope) > 0 && len(collection) > 0 {
		scopeCollPrefix = scope + "." + collection
	}

	for t, dm := range im.TypeMapping {
		if len(scopeCollPrefix) > 0 && !strings.HasPrefix(t, scopeCollPrefix) {
			// Do not consider this mapping as it's not applicable
			// to the relevant scope.collection
			continue
		}

		// Optimization when any of the top-level mappings is dynamic
		// and enabled, build index mapping based on search fields
		// if and only if there's no sub fields defined.
		// Return right away.
		if dm.Enabled && dm.Dynamic &&
			len(dm.Fields) == 0 && len(dm.Properties) == 0 {
			defaultAnalyzer := dm.DefaultAnalyzer
			if defaultAnalyzer == "" {
				defaultAnalyzer = im.DefaultAnalyzer
			}
			optimizedMapping := util.BuildIndexMappingOnFields(queryFields,
				defaultAnalyzer, im.DefaultDateTimeParser)
			// MB-52263
			// Update only default mapping to optimized definition's default mapping;
			// This is to retain any custom analysis components' definitions.
			rv.DefaultMapping = optimizedMapping.DefaultMapping
			return &rv
		}

		dmOptimized := optimizeDocumentMapping(queryFields,
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

	rv.DefaultMapping = optimizeDocumentMapping(queryFields,
		nil, im.DefaultMapping, im.DefaultAnalyzer, im.DefaultDateTimeParser)

	return &rv
}

func optimizeDocumentMapping(queryFields map[util.SearchField]struct{},
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

				_, exists := queryFields[util.SearchField{
					Name:       ftrack,
					Type:       f.Type,
					Analyzer:   analyzer,
					DateFormat: dateFormat,
				}]
				if !exists {
					_, exists = queryFields[util.SearchField{
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
		propDMOptimized := optimizeDocumentMapping(queryFields,
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
