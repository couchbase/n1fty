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
	"math"
	"strings"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
)

type SearchField struct {
	Name       string // Ex: "desc", "addr.geo.lat".
	Type       string
	Analyzer   string
	DateFormat string
}

// Strings is a wrapper that allows for a nil (pointer) value that's
// distinct from an empty []string.
type Strings struct {
	S []string
}

// DisallowedChars represents disallowed characters such as within
// type field names and docId prefixes.
var DisallowedChars = "\"\\%"

type ProcessedIndexParams struct {
	IndexMapping          *mapping.IndexMappingImpl
	DocConfig             *cbft.BleveDocumentConfig
	SearchFields          map[SearchField]bool
	IndexedCount          int64
	CondExpr              string
	DynamicMappings       map[string]string // Default Analyzers of enabled dynamic mappings
	AllFieldSearchable    bool
	DefaultAnalyzer       string
	DefaultDateTimeParser string
	MultipleTypeStrs      bool
}

// ProcessIndexDef determines if an indexDef is supportable as an
// datastore.FTSIndex, especially by invoking ProcessIndexMapping().
// The DocConfig.Mode is also checked here with current support for
// "type_field" and "docid_prefix" modes.
func ProcessIndexDef(indexDef *cbgt.IndexDef) (pip ProcessedIndexParams, err error) {
	// Other types like "fulltext-alias" are not-FTSIndex'able for now.
	if indexDef.Type != "fulltext-index" {
		return
	}

	bp := cbft.NewBleveParams()
	err = json.Unmarshal([]byte(indexDef.Params), bp)
	if err != nil {
		return pip, err
	}

	im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		return
	}

	var condExpr string

	switch bp.DocConfig.Mode {
	case "type_field":
		typeField := bp.DocConfig.TypeField
		if len(typeField) <= 0 ||
			strings.ContainsAny(typeField, DisallowedChars) {
			return
		}

		var multipleTypeStrs bool
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStrs != nil {
			if len(typeStrs.S) == 1 {
				// single type mapping
				if len(typeStrs.S[0]) == 0 || strings.ContainsAny(typeStrs.S[0], "\"\\") {
					return
				}

				// Ex: condExpr == '`type`="beer"'.
				condExpr = "`" + typeField + "`" + "=\"" + typeStrs.S[0] + "\""
			} else if len(typeStrs.S) > 1 {
				// multiple type mappings, supported only with FLEX
				multipleTypeStrs = true
				for i := range typeStrs.S {
					if len(typeStrs.S[i]) == 0 || strings.ContainsAny(typeStrs.S[i], "\"\\") {
						return
					}

					// Ex: condExpr: '`type` IN ["beer", "brewery"]'.
					if len(condExpr) == 0 {
						condExpr = "`" + typeField + "` IN [\"" + typeStrs.S[i] + "\""
					} else {
						condExpr += ", \"" + typeStrs.S[i] + "\""
					}
				}

				if len(condExpr) > 0 {
					condExpr += "]"
				}
			}
		}

		return ProcessedIndexParams{
			IndexMapping:          im,
			DocConfig:             &bp.DocConfig,
			SearchFields:          m,
			IndexedCount:          indexedCount,
			CondExpr:              condExpr,
			DynamicMappings:       dynamicMappings,
			AllFieldSearchable:    allFieldSearchable,
			DefaultAnalyzer:       defaultAnalyzer,
			DefaultDateTimeParser: defaultDateTimeParser,
			MultipleTypeStrs:      multipleTypeStrs,
		}, nil

	case "docid_prefix":
		dc := &bp.DocConfig

		if len(dc.DocIDPrefixDelim) != 1 ||
			strings.ContainsAny(dc.DocIDPrefixDelim, DisallowedChars) {
			return
		}

		var multipleTypeStrs bool
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStrs != nil {
			if len(typeStrs.S) > 1 {
				multipleTypeStrs = true
			}
			for i := range typeStrs.S {
				if strings.ContainsAny(typeStrs.S[i], DisallowedChars) ||
					strings.ContainsAny(typeStrs.S[i], dc.DocIDPrefixDelim) {
					return
				}

				// Ex: condExpr == 'META().id LIKE "beer-%" OR META().id LIKE "brewery-%"'.
				if len(condExpr) == 0 {
					condExpr = `META().id LIKE "` + typeStrs.S[i] + dc.DocIDPrefixDelim + `%"`
				} else {
					condExpr += ` OR META().id LIKE "` + typeStrs.S[i] + dc.DocIDPrefixDelim + `%"`
				}
			}
		}

		return ProcessedIndexParams{
			IndexMapping:          im,
			DocConfig:             dc,
			SearchFields:          m,
			IndexedCount:          indexedCount,
			CondExpr:              condExpr,
			DynamicMappings:       dynamicMappings,
			AllFieldSearchable:    allFieldSearchable,
			DefaultAnalyzer:       defaultAnalyzer,
			DefaultDateTimeParser: defaultDateTimeParser,
			MultipleTypeStrs:      multipleTypeStrs,
		}, nil

	case "docid_regexp":
		// N1QL doesn't currently support a generic regexp-based
		// condExpr, so not-FTSIndex'able.
		return

	default:
		return
	}
}

// ProcessIndexMapping currently checks the index mapping for two
// limited, simple cases of datastore.FTSIndex supportability...
//
// A) there's only an enabled default mapping (with no other type
//    mappings), where the returned typeStr will be nil.
//
// B) more than one type mapping is OK for as long as the default
//    mapping is NOT enabled, where typeStrs will be for example ..
//    &Strings{["beer", "brewery", ..]}
func ProcessIndexMapping(im *mapping.IndexMappingImpl) (m map[SearchField]bool,
	indexedCount int64, typeStrs *Strings, dynamicMappings map[string]string,
	allFieldSearchable bool, defaultAnalyzer string, defaultDateTimeParser string) {
	var ok bool
	dynamicMappings = map[string]string{}

	m = map[SearchField]bool{}

	var typeMappings int
	for t, tm := range im.TypeMapping {
		typeMappings++
		if tm.Enabled {
			m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
				im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
				nil, tm, m, 0)
			if !ok {
				return nil, 0, nil, nil, false, "", ""
			}

			if typeStrs == nil {
				typeStrs = &Strings{}
			}
			typeStrs.S = append(typeStrs.S, t)
			if tm.Dynamic {
				if tm.DefaultAnalyzer != "" {
					dynamicMappings[t] = tm.DefaultAnalyzer
				} else {
					dynamicMappings[t] = im.DefaultAnalyzer
				}
			}
		}
	}

	if im.DefaultMapping != nil && im.DefaultMapping.Enabled {
		// Saw both type mapping(s) & default mapping, so not-FTSIndex'able.
		if typeMappings > 0 {
			return nil, 0, nil, nil, false, "", ""
		}

		m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
			im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
			nil, im.DefaultMapping, m, 0)
		if !ok {
			return nil, 0, nil, nil, false, "", ""
		}

		typeStrs = nil
		if im.DefaultMapping.Dynamic {
			if im.DefaultMapping.DefaultAnalyzer != "" {
				dynamicMappings["default"] = im.DefaultMapping.DefaultAnalyzer
			} else {
				dynamicMappings["default"] = im.DefaultAnalyzer
			}
		}
	}

	if len(dynamicMappings) > 0 {
		// If one of the index's top level mappings is dynamic, the _all field
		// will contain every field's content.
		allFieldSearchable = true
	}

	if len(m) == 0 && len(dynamicMappings) == 0 {
		// No indexed fields or dynamic mappings
		return nil, 0, nil, nil, false, "", ""
	}

	return m, indexedCount, typeStrs, dynamicMappings,
		allFieldSearchable, im.DefaultAnalyzer, im.DefaultDateTimeParser
}

func ProcessDocumentMapping(im *mapping.IndexMappingImpl,
	defaultAnalyzer, defaultDateTimeParser string,
	path []string, dm *mapping.DocumentMapping, m map[SearchField]bool,
	indexedCount int64) (map[SearchField]bool, int64, bool, bool) {
	var allFieldSearchable, ok bool
	if !dm.Enabled {
		return m, indexedCount, allFieldSearchable, true
	}

	if m == nil {
		m = map[SearchField]bool{}
	}

	if dm.DefaultAnalyzer != "" {
		defaultAnalyzer = dm.DefaultAnalyzer
	}

	for _, f := range dm.Fields {
		if !f.Index || len(path) <= 0 {
			continue
		}

		fpath := append([]string(nil), path...) // Copy.
		fpath[len(fpath)-1] = f.Name

		searchField := SearchField{
			Name: strings.Join(fpath, "."),
			Type: f.Type,
		}

		if f.Type == "text" {
			searchField.Analyzer = f.Analyzer
			if searchField.Analyzer == "" {
				searchField.Analyzer = defaultAnalyzer
			}
		} else if f.Type == "datetime" {
			searchField.DateFormat = f.DateFormat
			if searchField.DateFormat == "" {
				searchField.DateFormat = defaultDateTimeParser
			}
		}

		if _, exists := m[searchField]; exists {
			continue
		}

		if indexedCount != math.MaxInt64 {
			indexedCount++
		}

		if f.IncludeInAll {
			allFieldSearchable = true
		}

		m[searchField] = false

		// Additionally include the same search field with the top-level
		// default_analyzer as well, to allow queries without the
		// "analyzer" setting (MB-33821)
		m[SearchField{
			Name:     searchField.Name,
			Type:     searchField.Type,
			Analyzer: im.DefaultAnalyzer,
		}] = false
	}

	for prop, propDM := range dm.Properties {
		if propDM.DefaultAnalyzer != "" {
			defaultAnalyzer = propDM.DefaultAnalyzer
		}
		m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
			im, defaultAnalyzer, defaultDateTimeParser,
			append(path, prop), propDM, m, indexedCount)
		if !ok {
			return nil, 0, false, false
		}
	}

	if dm.Dynamic {
		allFieldSearchable = true
		searchField := SearchField{
			Name:     strings.Join(path, "."),
			Analyzer: defaultAnalyzer,
		}

		if _, exists := m[searchField]; !exists {
			m[searchField] = true
			indexedCount = math.MaxInt64
		}
	}

	return m, indexedCount, allFieldSearchable, true
}

// -----------------------------------------------------------------------------

func FetchFieldsToSearchFromQuery(que query.Query) (map[SearchField]struct{}, error) {
	queryFields := map[SearchField]struct{}{}

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
				case *query.GeoBoundingBoxQuery,
					*query.GeoDistanceQuery,
					*query.GeoBoundingPolygonQuery:
					fieldDesc.Type = "geopoint"
				case *query.MatchQuery:
					fieldDesc.Type = "text"
					fieldDesc.Analyzer = qqq.Analyzer
				case *query.MatchPhraseQuery:
					fieldDesc.Type = "text"
					fieldDesc.Analyzer = qqq.Analyzer
				default:
					// The analyzer expectation for the following queries is keyword:
					//   - *query.TermQuery
					//   - *query.PhraseQuery
					//   - *query.MultiPhraseQuery
					//   - *query.FuzzyQuery
					//   - *query.PrefixQuery
					//   - *query.RegexpQuery
					//   - *query.WildcardQuery
					fieldDesc.Type = "text"
					fieldDesc.Analyzer = "keyword"
				}

				queryFields[fieldDesc] = struct{}{}
			}
			// The following are non-Fieldable queries:
			//   - *query.DocIDQuery
			//   - *query.MatchAllQuery
			//   - *query.MatchNoneQuery
		}
	}

	walk(que)

	return queryFields, nil
}
