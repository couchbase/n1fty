// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package util

import (
	"encoding/json"
	"math"
	"strings"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
)

type SearchField struct {
	Name       string // Ex: "desc", "addr.geo.lat".
	Type       string
	Analyzer   string
	DateFormat string
}

// Types is a wrapper that allows for a nil (pointer) value that's
// distinct from an empty map[string]bool.
type Types struct {
	S map[string]bool // Map of type-mapping name to whether it is enabled or not
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
	TypeMappings          []string
	Scope                 string
	Collection            string
}

// ProcessIndexDef determines if an indexDef is supportable as an
// datastore.FTSIndex, especially by invoking ProcessIndexMapping().
// The DocConfig.Mode is also checked here with current support for
// "type_field", "docid_prefix" and "docid_regexp" modes.
func ProcessIndexDef(indexDef *cbgt.IndexDef, scope, collection string) (
	pip ProcessedIndexParams, err error) {
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

		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		var typeMappings []string
		if typeStrs != nil {
			for typeMapping, enabled := range typeStrs.S {
				if len(typeMapping) == 0 || strings.ContainsAny(typeMapping, "\"\\") {
					return
				}

				if enabled {
					typeMappings = append(typeMappings, typeMapping)
				}
			}

			if len(typeMappings) == 1 {
				// Ex: condExpr == '`type`="beer"'.
				condExpr = "`" + typeField + "`" + "=\"" + typeMappings[0] + "\""
			} else if len(typeMappings) > 1 {
				for i := range typeMappings {
					// Ex: condExpr: '`type` IN ["beer", "brewery"]'.
					if len(condExpr) == 0 {
						condExpr = "`" + typeField + "` IN [\"" + typeMappings[i] + "\""
					} else {
						condExpr += ", \"" + typeMappings[i] + "\""
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
			TypeMappings:          typeMappings,
		}, nil

	case "docid_prefix":
		dc := &bp.DocConfig

		if len(dc.DocIDPrefixDelim) == 0 ||
			strings.ContainsAny(dc.DocIDPrefixDelim, DisallowedChars) {
			return
		}

		var typeMappings []string
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStrs != nil {
			for typeMapping, enabled := range typeStrs.S {
				if !enabled {
					continue
				}

				if strings.ContainsAny(typeMapping, DisallowedChars) ||
					strings.ContainsAny(typeMapping, dc.DocIDPrefixDelim) {
					return
				}

				typeMappings = append(typeMappings, typeMapping)
				// Ex: condExpr == 'META().id LIKE "beer-%" OR META().id LIKE "brewery-%"'.
				if len(condExpr) == 0 {
					condExpr = `META().id LIKE "` + typeMapping + dc.DocIDPrefixDelim + `%"`
				} else {
					condExpr += ` OR META().id LIKE "` + typeMapping + dc.DocIDPrefixDelim + `%"`
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
			TypeMappings:          typeMappings,
		}, nil

	case "docid_regexp":
		dc := &bp.DocConfig

		var typeMappings []string
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		if typeStrs != nil {
			for typeMapping, enabled := range typeStrs.S {
				if !enabled {
					continue
				}

				if strings.ContainsAny(typeMapping, DisallowedChars) {
					return
				}

				typeMappings = append(typeMappings, typeMapping)
				// Ex: condExpr == 'META().id LIKE "%beer" OR META().id LIKE "%brewery%"'.
				if len(condExpr) == 0 {
					condExpr = `META().id LIKE "%` + typeMapping + `%"`
				} else {
					condExpr += `OR META().id LIKE "%` + typeMapping + `%"`
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
			TypeMappings:          typeMappings,
		}, nil

	case "scope.collection.type_field":
		typeField := bp.DocConfig.TypeField
		if len(typeField) <= 0 ||
			strings.ContainsAny(typeField, DisallowedChars) {
			return
		}

		var typeMappings []string
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		var entireScopeCollIndexed bool
		if typeStrs != nil {
			scopeCollTypes := map[string]bool{}
			for typeMapping, enabled := range typeStrs.S {
				if len(typeMapping) == 0 || strings.ContainsAny(typeMapping, "\"\\") {
					return
				}

				arr := strings.SplitN(typeMapping, ".", 3)
				if len(arr) == 1 {
					if (scope == "" || scope == "_default") &&
						(collection == "" || collection == "_default") {
						scopeCollTypes[arr[0]] = enabled
					}
				} else if len(arr) == 2 {
					if scope == arr[0] && collection == arr[1] {
						entireScopeCollIndexed = enabled
					}
				} else if len(arr) == 3 {
					if scope == arr[0] && collection == arr[1] {
						scopeCollTypes[arr[2]] = enabled
					}
				}
			}

			if entireScopeCollIndexed {
				if len(scopeCollTypes) > 0 {
					// Do not consider this index, to avoid the possibility of false negatives.
					return
				}
				// condExpr is nil
			} else {
				for typeName, enabled := range scopeCollTypes {
					if enabled {
						typeMappings = append(typeMappings, typeName)
					}
				}

				if len(typeMappings) == 0 {
					// Do not consider index, as nothing relevant to the scope.collection is
					// indexed.
					return
				} else if len(typeMappings) == 1 {
					// Ex: condExpr == '`type`="beer"'
					condExpr = "`" + typeField + "`" + "=\"" + typeMappings[0] + "\""
				} else {
					for i := range typeMappings {
						// Ex: condExpr: '`type` IN ["beer", "brewery"]'.
						if len(condExpr) == 0 {
							condExpr = "`" + typeField + "` IN [\"" + typeMappings[i] + "\""
						} else {
							condExpr += ", \"" + typeMappings[i] + "\""
						}
					}
					condExpr += "]"
				}
			}
		}

		if entireScopeCollIndexed {
			indexedCount = math.MaxInt64
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
			TypeMappings:          typeMappings,
			Scope:                 scope,
			Collection:            collection,
		}, nil

	case "scope.collection.docid_prefix":
		dc := &bp.DocConfig

		if len(dc.DocIDPrefixDelim) == 0 ||
			strings.ContainsAny(dc.DocIDPrefixDelim, DisallowedChars) {
			return
		}

		var typeMappings []string
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		var entireScopeCollIndexed bool
		if typeStrs != nil {
			scopeCollTypes := map[string]bool{}
			for typeMapping, enabled := range typeStrs.S {
				if strings.ContainsAny(typeMapping, DisallowedChars) ||
					strings.ContainsAny(typeMapping, dc.DocIDPrefixDelim) {
					return
				}

				arr := strings.SplitN(typeMapping, ".", 3)
				if len(arr) == 1 {
					if (scope == "" || scope == "_default") &&
						(collection == "" || collection == "_default") {
						scopeCollTypes[arr[0]] = enabled
					}
				} else if len(arr) == 2 {
					if scope == arr[0] && collection == arr[1] {
						entireScopeCollIndexed = enabled
					}
				} else if len(arr) == 3 {
					if scope == arr[0] && collection == arr[1] {
						scopeCollTypes[arr[2]] = enabled
					}
				}
			}

			if entireScopeCollIndexed {
				if len(scopeCollTypes) > 0 {
					// Do not consider this index, to avoid the possibility of false negatives.
					return
				}
				// condExpr is nil
			} else {
				for typeName, enabled := range scopeCollTypes {
					if enabled {
						typeMappings = append(typeMappings, typeName)
					}
				}

				if len(typeMappings) == 0 {
					// Do not consider index, as nothing relevant to the scope.collection is
					// indexed.
					return
				} else {
					for i := range typeMappings {
						// Ex: condExpr == 'META().id LIKE "beer-%" OR META().id LIKE "brewery-%"'.
						if len(condExpr) == 0 {
							condExpr = `META().id LIKE "` + typeMappings[i] + dc.DocIDPrefixDelim + `%"`
						} else {
							condExpr += ` OR META().id LIKE "` + typeMappings[i] + dc.DocIDPrefixDelim + `%"`
						}
					}
				}
			}
		}

		if entireScopeCollIndexed {
			indexedCount = math.MaxInt64
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
			TypeMappings:          typeMappings,
			Scope:                 scope,
			Collection:            collection,
		}, nil

	case "scope.collection.docid_regexp":
		dc := &bp.DocConfig

		var typeMappings []string
		m, indexedCount, typeStrs, dynamicMappings, allFieldSearchable,
			defaultAnalyzer, defaultDateTimeParser := ProcessIndexMapping(im)
		var entireScopeCollIndexed bool
		if typeStrs != nil {
			scopeCollTypes := map[string]bool{}
			for typeMapping, enabled := range typeStrs.S {
				if strings.ContainsAny(typeMapping, DisallowedChars) {
					return
				}

				arr := strings.SplitN(typeMapping, ".", 3)
				if len(arr) == 1 {
					if (scope == "" || scope == "_default") &&
						(collection == "" || collection == "_default") {
						scopeCollTypes[arr[0]] = enabled
					}
				} else if len(arr) == 2 {
					if scope == arr[0] && collection == arr[1] {
						entireScopeCollIndexed = enabled
					}
				} else if len(arr) == 3 {
					if scope == arr[0] && collection == arr[1] {
						scopeCollTypes[arr[2]] = enabled
					}
				}
			}

			if entireScopeCollIndexed {
				if len(scopeCollTypes) > 0 {
					// Do not consider this index, to avoid the possibility of false negatives.
					return
				}
				// condExpr is nil
			} else {
				for typeName, enabled := range scopeCollTypes {
					if enabled {
						typeMappings = append(typeMappings, typeName)
					}
				}

				if len(typeMappings) == 0 {
					// Do not consider index, as nothing relevant to the scope.collection is
					// indexed.
					return
				} else {
					for i := range typeMappings {
						// Ex: condExpr == 'META().id LIKE "%beer" OR META().id LIKE "%brewery%"'.
						if len(condExpr) == 0 {
							condExpr = `META().id LIKE "%` + typeMappings[i] + `%"`
						} else {
							condExpr += ` OR META().id LIKE "%` + typeMappings[i] + `%"`
						}
					}
				}
			}
		}

		if entireScopeCollIndexed {
			indexedCount = math.MaxInt64
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
			TypeMappings:          typeMappings,
			Scope:                 scope,
			Collection:            collection,
		}, nil

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
//    &types{{"beer":true}, {"brewery":true}, ..]}
func ProcessIndexMapping(im *mapping.IndexMappingImpl) (m map[SearchField]bool,
	indexedCount int64, typeStrs *Types, dynamicMappings map[string]string,
	allFieldSearchable bool, defaultAnalyzer string, defaultDateTimeParser string) {
	var ok bool
	dynamicMappings = map[string]string{}

	m = map[SearchField]bool{}

	for t, tm := range im.TypeMapping {
		if typeStrs == nil {
			typeStrs = &Types{S: make(map[string]bool)}
		}
		if tm.Enabled {
			m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
				im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
				nil, tm, m, 0)
			if !ok {
				return nil, 0, nil, nil, false, "", ""
			}

			if tm.Dynamic {
				if tm.DefaultAnalyzer != "" {
					dynamicMappings[t] = tm.DefaultAnalyzer
				} else {
					dynamicMappings[t] = im.DefaultAnalyzer
				}
			}
		}
		typeStrs.S[t] = tm.Enabled
	}

	if im.DefaultMapping != nil && im.DefaultMapping.Enabled {
		// Saw both type mapping(s) & default mapping, so not-FTSIndex'able.
		if typeStrs != nil {
			return nil, 0, nil, nil, false, "", ""
		}

		m, indexedCount, allFieldSearchable, ok = ProcessDocumentMapping(
			im, im.DefaultAnalyzer, im.DefaultDateTimeParser,
			nil, im.DefaultMapping, m, 0)
		if !ok {
			return nil, 0, nil, nil, false, "", ""
		}

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

	var walk func(que query.Query) error

	walk = func(que query.Query) error {
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
		case *query.QueryStringQuery:
			q, err := qq.Parse()
			if err != nil {
				return err
			}
			walk(q)
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

		return nil
	}

	err := walk(que)
	if err != nil {
		return nil, err
	}

	return queryFields, nil
}
