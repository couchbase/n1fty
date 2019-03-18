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
	"strings"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
)

type SearchField struct {
	Name     string // Ex: "desc", "addr.geo.lat".
	Type     string
	Analyzer string
}

// String is a wrapper that allows for a nil (pointer) value that's
// distinct from the "" empty string value.
type String struct {
	S string
}

// DisallowedChars represents disallowed characters such as within
// type field names and docId prefixes.
var DisallowedChars = "\"\\%"

// ProcessIndexDef determines if an indexDef is supportable as an
// datastore.FTSIndex, especially by invoking ProcessIndexMapping().
// The DocConfig.Mode is also checked here with current support for
// "type_field" and "docid_prefix" modes.
func ProcessIndexDef(indexDef *cbgt.IndexDef) (
	imOut *mapping.IndexMappingImpl,
	docConfigOut *cbft.BleveDocumentConfig,
	m map[SearchField]bool, condExpr string, dynamicOut bool,
	defaultAnalyzerOut string, err error) {
	// Other types like "fulltext-alias" are not-FTSIndex'able for now.
	if indexDef.Type != "fulltext-index" {
		return nil, nil, nil, "", false, "", nil
	}

	bp := cbft.NewBleveParams()
	err = json.Unmarshal([]byte(indexDef.Params), bp)
	if err != nil {
		return nil, nil, nil, "", false, "", err
	}

	im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		return nil, nil, nil, "", false, "", nil
	}

	switch bp.DocConfig.Mode {
	case "type_field":
		typeField := bp.DocConfig.TypeField
		if len(typeField) <= 0 ||
			strings.ContainsAny(typeField, DisallowedChars) {
			return nil, nil, nil, "", false, "", nil
		}

		m, typeStr, dynamic, defaultAnalyzer := ProcessIndexMapping(im)
		if typeStr != nil {
			if len(typeStr.S) <= 0 ||
				strings.ContainsAny(typeStr.S, "\"\\") {
				return nil, nil, nil, "", false, "", nil
			}

			// Ex: condExpr == 'type="beer"'.
			condExpr = typeField + `="` + typeStr.S + `"`
		}

		return im, &bp.DocConfig, m, condExpr, dynamic, defaultAnalyzer, nil

	case "docid_prefix":
		dc := &bp.DocConfig

		if len(dc.DocIDPrefixDelim) != 1 ||
			strings.ContainsAny(dc.DocIDPrefixDelim, DisallowedChars) {
			return nil, nil, nil, "", false, "", nil
		}

		m, typeStr, dynamic, defaultAnalyzer := ProcessIndexMapping(im)
		if typeStr != nil {
			if len(typeStr.S) <= 0 ||
				strings.ContainsAny(typeStr.S, DisallowedChars) ||
				strings.ContainsAny(typeStr.S, dc.DocIDPrefixDelim) {
				return nil, nil, nil, "", false, "", nil
			}

			// Ex: condExpr == 'META().id LIKE "beer-%"'.
			condExpr = `META().id LIKE "` + typeStr.S + dc.DocIDPrefixDelim + `%"`
		}

		return im, dc, m, condExpr, dynamic, defaultAnalyzer, nil

	case "docid_regexp":
		// N1QL doesn't currently support a generic regexp-based
		// condExpr, so not-FTSIndex'able.
		return nil, nil, nil, "", false, "", nil

	default:
		return nil, nil, nil, "", false, "", nil
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
//
// TODO: we only support top-level dynamic right now, but
// might want to support nested level dynamic in the future?
func ProcessIndexMapping(im *mapping.IndexMappingImpl) (m map[SearchField]bool,
	typeStr *String, dynamic bool, defaultAnalyzer string) {
	var ok bool

	for t, tm := range im.TypeMapping {
		if tm.Enabled {
			if m != nil { // Saw 2nd enabled type mapping, so not-FTSIndex'able.
				return nil, nil, false, ""
			}

			m, defaultAnalyzer, ok = ProcessDocumentMapping(
				im.DefaultAnalyzer, nil, tm, nil)
			if !ok {
				return nil, nil, false, ""
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
			return nil, nil, false, ""
		}

		m, defaultAnalyzer, ok = ProcessDocumentMapping(
			im.DefaultAnalyzer, nil, im.DefaultMapping, nil)
		if !ok {
			return nil, nil, false, ""
		}

		typeStr = nil
		dynamic = im.DefaultMapping.Dynamic
	}

	if len(m) <= 0 {
		return nil, nil, false, ""
	}

	return m, typeStr, dynamic, defaultAnalyzer
}

func ProcessDocumentMapping(defaultAnalyzer string, path []string,
	dm *mapping.DocumentMapping, m map[SearchField]bool) (
	mOut map[SearchField]bool, defaultAnalyzerOut string, ok bool) {
	if !dm.Enabled {
		return m, defaultAnalyzer, true
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
		}

		if _, exists := m[searchField]; exists {
			return nil, "", false
		}

		m[searchField] = false
	}

	for prop, propDM := range dm.Properties {
		m, _, ok = ProcessDocumentMapping(defaultAnalyzer,
			append(path, prop), propDM, m)
		if !ok {
			return nil, "", false
		}
	}

	if dm.Dynamic {
		searchField := SearchField{
			Name:     strings.Join(path, "."),
			Analyzer: defaultAnalyzer,
		}

		if _, exists := m[searchField]; exists {
			return nil, "", false
		}

		m[searchField] = true
	}

	return m, defaultAnalyzer, true
}

// -----------------------------------------------------------------------------

func FetchFieldsToSearchFromQuery(que query.Query) ([]SearchField, error) {
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
