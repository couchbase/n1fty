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

// String is a wrapper that allows for a nil (pointer) value that's
// distinct from the "" empty string value.
type String struct {
	S string
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
	Dynamic               bool
	AllFieldSearchable    bool
	DefaultAnalyzer       string
	DefaultDateTimeParser string
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

		m, indexedCount, typeStr, dynamic, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStr != nil {
			if len(typeStr.S) <= 0 ||
				strings.ContainsAny(typeStr.S, "\"\\") {
				return
			}

			// Ex: condExpr == '`type`="beer"'.
			condExpr = "`" + typeField + "`" + "=\"" + typeStr.S + "\""
		}

		return ProcessedIndexParams{
			IndexMapping:          im,
			DocConfig:             &bp.DocConfig,
			SearchFields:          m,
			IndexedCount:          indexedCount,
			CondExpr:              condExpr,
			Dynamic:               dynamic,
			AllFieldSearchable:    allFieldSearchable,
			DefaultAnalyzer:       defaultAnalyzer,
			DefaultDateTimeParser: defaultDateTimeParser,
		}, nil

	case "docid_prefix":
		dc := &bp.DocConfig

		if len(dc.DocIDPrefixDelim) != 1 ||
			strings.ContainsAny(dc.DocIDPrefixDelim, DisallowedChars) {
			return
		}

		m, indexedCount, typeStr, dynamic, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStr != nil {
			if len(typeStr.S) <= 0 ||
				strings.ContainsAny(typeStr.S, DisallowedChars) ||
				strings.ContainsAny(typeStr.S, dc.DocIDPrefixDelim) {
				return
			}

			// Ex: condExpr == 'META().id LIKE "beer-%"'.
			condExpr = `META().id LIKE "` + typeStr.S + dc.DocIDPrefixDelim + `%"`
		}

		return ProcessedIndexParams{
			IndexMapping:          im,
			DocConfig:             dc,
			SearchFields:          m,
			IndexedCount:          indexedCount,
			CondExpr:              condExpr,
			Dynamic:               dynamic,
			AllFieldSearchable:    allFieldSearchable,
			DefaultAnalyzer:       defaultAnalyzer,
			DefaultDateTimeParser: defaultDateTimeParser,
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
// B) there's just 1 enabled type mapping (with no default mapping),
//    where returned typeStr will be, for example, &String{"beer"}.
//
// TODO: support multiple enabled type mappings some future day,
// which would likely change the func signature here.
func ProcessIndexMapping(im *mapping.IndexMappingImpl) (m map[SearchField]bool,
	indexedCount int64, typeStr *String, dynamic bool, allFieldSearchable bool,
	defaultAnalyzer string, defaultDateTimeParser string) {
	var ok bool

	for t, tm := range im.TypeMapping {
		if tm.Enabled {
			if m != nil { // Saw 2nd enabled type mapping, so not-FTSIndex'able.
				return nil, 0, nil, false, false, "", ""
			}

			m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
				im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
				nil, tm, nil, 0)
			if !ok {
				return nil, 0, nil, false, false, "", ""
			}

			typeStr = nil
			dynamic = false
			if m != nil {
				typeStr = &String{t}
				dynamic = tm.Dynamic
			}
		}
	}

	if im.DefaultMapping != nil && im.DefaultMapping.Enabled {
		// Saw both type mapping(s) & default mapping, so not-FTSIndex'able.
		if len(im.TypeMapping) > 0 {
			return nil, 0, nil, false, false, "", ""
		}

		m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
			im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
			nil, im.DefaultMapping, nil, 0)
		if !ok {
			return nil, 0, nil, false, false, "", ""
		}

		typeStr = nil
		dynamic = im.DefaultMapping.Dynamic
	}

	if dynamic {
		// If one of the index's top level mappings is dynamic, the _all field
		// will contain every field's content.
		allFieldSearchable = true
	}

	if len(m) <= 0 {
		return nil, 0, nil, false, false, "", ""
	}

	return m, indexedCount, typeStr, dynamic, allFieldSearchable,
		im.DefaultAnalyzer, im.DefaultDateTimeParser
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

				queryFields[fieldDesc] = struct{}{}
			}
		}
	}

	walk(que)

	return queryFields, nil
}
