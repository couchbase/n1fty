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

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/logging"
)

type SearchField struct {
	Name     string
	Type     string
	Analyzer string
}

func SearchableFieldsForIndexDef(indexDef *cbgt.IndexDef) (
	map[SearchField]bool, bool, string) {
	bp := cbft.NewBleveParams()
	err := json.Unmarshal([]byte(indexDef.Params), bp)
	if err != nil {
		logging.Infof("n1fty: skip indexDef: %+v,"+
			" json unmarshal indexDef.Params, err: %v\n", indexDef, err)
		return nil, false, ""
	}

	if bp.DocConfig.Mode != "type_field" {
		logging.Infof("n1fty: skip indexDef: %+v,"+
			" wrong DocConfig.Mode\n", indexDef)
		return nil, false, ""
	}

	typeField := bp.DocConfig.TypeField
	if typeField == "" {
		logging.Infof("n1fty: skip indexDef: %+v,"+
			" wrong DocConfig.TypeField\n", typeField)
		return nil, false, ""
	}

	im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		logging.Infof("n1fty: skip indexDef: %+v, "+
			" not IndexMappingImpl\n", *indexDef)
		return nil, false, ""
	}

	// set this index mapping into the indexMappings cache
	SetIndexMapping(indexDef.Name, &MappingDetails{
		UUID:       indexDef.UUID,
		SourceName: indexDef.SourceName,
		IMapping:   im,
	})

	return searchableFieldsForIndexMappingImpl(im)
}

func SearchableFieldsForIndexMapping(im mapping.IndexMapping) (
	map[SearchField]bool, bool, string) {
	m, ok := im.(*mapping.IndexMappingImpl)
	if !ok {
		logging.Infof("n1fty: index mapping: %+v, "+
			" not IndexMappingImpl\n", m)
		return nil, false, ""
	}

	return searchableFieldsForIndexMappingImpl(m)
}

func searchableFieldsForIndexMappingImpl(bm *mapping.IndexMappingImpl) (
	map[SearchField]bool, bool, string) {
	searchFieldsMap := map[SearchField]bool{}

	var dynamicMapping bool
	for _, typeMapping := range bm.TypeMapping {
		if typeMapping.Enabled {
			analyzer := typeMapping.DefaultAnalyzer
			if analyzer == "" {
				analyzer = bm.DefaultAnalyzer
			}
			if typeMapping.Dynamic {
				if analyzer == bm.DefaultAnalyzer {
					// everything under document type is indexed
					dynamicMapping = true
				}
			} else {
				searchFieldsMap = fetchSearchableFields("", typeMapping,
					searchFieldsMap, analyzer)
			}
		}
	}

	if bm.DefaultMapping != nil && bm.DefaultMapping.Enabled {
		analyzer := bm.DefaultMapping.DefaultAnalyzer
		if analyzer == "" {
			analyzer = bm.DefaultAnalyzer
		}
		if bm.DefaultMapping.Dynamic {
			if analyzer == bm.DefaultAnalyzer {
				// everything under document type is indexed
				dynamicMapping = true
			}
		} else {
			searchFieldsMap = fetchSearchableFields("", bm.DefaultMapping,
				searchFieldsMap, analyzer)
		}
	}

	return searchFieldsMap, dynamicMapping, bm.DefaultAnalyzer
}

func fetchSearchableFields(path string,
	typeMapping *mapping.DocumentMapping,
	searchFieldsMap map[SearchField]bool,
	parentAnalyzer string) map[SearchField]bool {
	if !typeMapping.Enabled {
		return searchFieldsMap
	}

	if typeMapping.Dynamic &&
		len(typeMapping.Fields) == 0 &&
		len(typeMapping.Properties) >= 0 {
		analyzer := typeMapping.DefaultAnalyzer
		if analyzer == "" {
			analyzer = parentAnalyzer
		}
		searchFieldsMap[SearchField{
			Name:     path,
			Analyzer: analyzer,
		}] = true
		return searchFieldsMap
	}

	for _, field := range typeMapping.Fields {
		fieldName := field.Name
		if len(path) > 0 {
			fieldName = path + "." + fieldName
		}
		searchField := SearchField{
			Name: fieldName,
			Type: field.Type,
		}
		if field.Type == "text" {
			// analyzer is applicable only when field type is "text"
			analyzer := field.Analyzer
			if analyzer == "" {
				// apply parent analyzer if analyzer not specified
				analyzer = parentAnalyzer
			}
			searchField.Analyzer = analyzer
		}
		if _, exists := searchFieldsMap[searchField]; !exists {
			searchFieldsMap[searchField] = false
		}
	}

	for childMappingName, childMapping := range typeMapping.Properties {
		newPath := path
		if len(childMapping.Fields) == 0 {
			if len(path) == 0 {
				newPath = childMappingName
			} else {
				newPath += "." + childMappingName
			}
		}
		analyzer := childMapping.DefaultAnalyzer
		if analyzer == "" {
			analyzer = parentAnalyzer
		}
		searchFieldsMap = fetchSearchableFields(newPath, childMapping,
			searchFieldsMap, analyzer)
	}

	return searchFieldsMap
}

// -----------------------------------------------------------------------------

func FetchFieldsToSearchFromQuery(q []byte) ([]SearchField, error) {
	que, err := query.ParseQuery(q)
	if err != nil {
		return nil, err
	}

	fields := []SearchField{}
	var walk func(que query.Query)

	walk = func(que query.Query) {
		switch qq := que.(type) {
		case *query.BooleanQuery:
			walk(qq.Must)
			walk(qq.MustNot)
			walk(qq.Should)
		case *query.ConjunctionQuery:
			for _, childQ := range qq.Conjuncts {
				walk(childQ)
			}
		case *query.DisjunctionQuery:
			for _, childQ := range qq.Disjuncts {
				walk(childQ)
			}
		default:
			if fq, ok := que.(query.FieldableQuery); ok {

				fieldDesc := SearchField{
					Name: fq.Field(),
				}

				switch qqq := fq.(type) {
				case *query.BoolFieldQuery:
					fieldDesc.Type = "boolean"
				case *query.NumericRangeQuery:
					fieldDesc.Type = "number"
				case *query.DateRangeQuery:
					fieldDesc.Type = "datetime"
				case *query.GeoBoundingBoxQuery, *query.GeoDistanceQuery:
					fieldDesc.Type = "geopoint"
				case *query.MatchQuery:
					fieldDesc.Type = "text"
					fieldDesc.Analyzer = qqq.Analyzer
				case *query.MatchPhraseQuery:
					fieldDesc.Type = "text"
					fieldDesc.Analyzer = qqq.Analyzer
				default:
					fieldDesc.Type = "text"
				}

				fields = append(fields, fieldDesc)
			}
		}
	}

	walk(que)
	return fields, nil
}
