//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package flex

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/couchbase/cbft"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var DefaultTypeFieldPath = []string{"type"}

// Extracts scope, collection and type names typeMapping:
//     "scope.collection.type" => "scope", "collection", "type"
//     "scope.collection"      => "scope", "collection", ""
//     "type"                  => "_default", "_default", "type"
func extractScopeCollTypeNames(t string) (string, string, string) {
	scopeCollType := strings.SplitN(t, ".", 3)
	if len(scopeCollType) == 1 {
		return "_default", "_default", scopeCollType[0]
	} else if len(scopeCollType) == 2 {
		// typeName is empty (subscribing to the entire scope.collection)
		return scopeCollType[0], scopeCollType[1], ""
	}
	return scopeCollType[0], scopeCollType[1], scopeCollType[2]
}

// BleveToCondFlexIndexes translates a bleve index into CondFlexIndexes.
// NOTE: checking for DocConfig.Mode should be done beforehand.
func BleveToCondFlexIndexes(name, uuid string, im *mapping.IndexMappingImpl,
	docConfig *cbft.BleveDocumentConfig, scope, collection string) (
	rv CondFlexIndexes, err error) {
	if im == nil {
		return nil, fmt.Errorf("index mapping not available")
	}

	collectionAware := len(scope) > 0 && len(collection) > 0
	// Map of FieldTrack => fieldType => count.
	fieldTrackTypeCounts := map[FieldTrack]map[string]int{}
	// Map of FieldTrack => array of type mappings within which they occur
	fieldTrackTypes := map[FieldTrack]map[string]struct{}{}
	// Array of type mapping names
	types := make([]string, 0, len(im.TypeMapping))
	for t, dm := range im.TypeMapping {
		typeName := t
		if collectionAware {
			sName, cName, tName := extractScopeCollTypeNames(t)
			if sName != scope || cName != collection {
				continue
			}
			typeName = tName
		}
		types = append(types, t)
		countFieldTrackTypes(nil, typeName, dm, im.DefaultAnalyzer,
			fieldTrackTypeCounts, fieldTrackTypes)
	}
	countFieldTrackTypes(nil, "", im.DefaultMapping, im.DefaultAnalyzer,
		fieldTrackTypeCounts, fieldTrackTypes)

	sort.Strings(types) // For output stability.

	fi := &FlexIndex{
		Name:             name,
		UUID:             uuid,
		IndexedFields:    FieldInfos{},
		SortableFields:   []string{},
		SupportedExprs:   []SupportedExpr{},
		DocValuesDynamic: im.DocValuesDynamic,
		StoreDynamic:     im.StoreDynamic,
	}

	if len(fieldTrackTypes) > 0 {
		fi.FieldTrackTypes = fieldTrackTypes
	}

	typeFieldPath := DefaultTypeFieldPath
	mode := "type_field"
	if docConfig != nil {
		mode = docConfig.Mode
		switch mode {
		case "type_field", "scope.collection.type_field":
			if len(docConfig.TypeField) > 0 {
				typeFieldPath = []string{docConfig.TypeField}
			}
			fi.IndexedFields = append(fi.IndexedFields,
				&FieldInfo{FieldPath: typeFieldPath, FieldType: "text"})
		case "docid_prefix", "scope.collection.docid_prefix",
			"docid_regexp", "scope.collection.docid_regexp":
			typeFieldPath = []string{"meta().`id`"}
		default:
			return nil, fmt.Errorf("unsupported docConfig.mode: %v", mode)
		}
	}

	values := map[value.Value]*valueDetails{}
	var skipCondFuncEqCheck bool
	for _, t := range types {
		typeEqEffect := "FlexBuild:n" // Strips `type = "BEER"` from expressions.
		if !im.TypeMapping[t].Enabled {
			typeEqEffect = "not-sargable"
		}

		typeName := t
		if collectionAware {
			// scope, collection previously validated
			_, _, typeName = extractScopeCollTypeNames(t)
		}

		if len(typeName) > 0 {
			// Cond (FlexBuild:n) expression available only when typeName is available in
			// the type mapping, this is the scenario where index is subscibing to a
			// specifc "type" of documents within the scope.collection.

			if mode == "type_field" || mode == "scope.collection.type_field" {
				val := value.NewValue(typeName)

				// Strips `type = "BEER"` from expressions.
				fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
					Cmp:       "eq",
					FieldPath: typeFieldPath,
					ValueType: "text",
					ValueMust: val,
					Effect:    typeEqEffect,
				})

				// To treat `type > "BEER"` as not-sargable.
				fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
					Cmp:       "lt gt le ge",
					FieldPath: typeFieldPath,
					ValueType: "", // Treated as not-sargable.
					Effect:    "not-sargable",
				})

				values[val] = &valueDetails{
					typeName: typeName,
					cmp:      "eq",
				}
			} else if mode == "docid_prefix" || mode == "scope.collection.docid_prefix" {
				val := value.NewValue(typeName + docConfig.DocIDPrefixDelim + "%")

				// Strips `meta().id LIKE "BEER-%"` from expressions.
				fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
					Cmp:       "like",
					FieldPath: typeFieldPath,
					ValueType: "text",
					ValueMust: val,
					Effect:    typeEqEffect,
				})

				values[val] = &valueDetails{
					typeName: typeName,
					cmp:      "like",
				}
			} else if mode == "docid_regexp" || mode == "scope.collection.docid_regexp" {
				val := value.NewValue("%" + typeName + "%")

				// Strips `meta().id LIKE "%BEER%"` from expressions.
				fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
					Cmp:       "like",
					FieldPath: typeFieldPath,
					ValueType: "text",
					ValueMust: val,
					Effect:    typeEqEffect,
				})

				values[val] = &valueDetails{
					typeName: typeName,
					cmp:      "like",
				}
			}
		} else {
			// No Cond expression for this FlexIndex as it's subscribing to the entire
			// scope.collection.
			skipCondFuncEqCheck = true
		}

		fi, err = BleveToFlexIndex(fi, nil, im.TypeMapping[t], im.DefaultAnalyzer,
			fieldTrackTypeCounts)
		if err != nil {
			return nil, err
		}
	}

	if len(fi.SupportedExprs) > 0 {
		// Add CondFlexIndex over all the types, iff at least one type mapping
		// is enabled
		if len(values) > 1 {
			fi.MultipleTypeStrs = true
		}
		rv = append(rv, &CondFlexIndex{
			Cond:      MakeCondFuncEqVals(typeFieldPath, values, skipCondFuncEqCheck),
			FlexIndex: fi,
		})
	}

	if im.DefaultMapping != nil && im.DefaultMapping.Enabled {
		fi, err := BleveToFlexIndex(&FlexIndex{
			IndexedFields:    FieldInfos{},
			DocValuesDynamic: im.DocValuesDynamic,
			StoreDynamic:     im.StoreDynamic,
		}, nil, im.DefaultMapping, im.DefaultAnalyzer, fieldTrackTypeCounts)
		if err != nil {
			return nil, err
		}

		if len(fieldTrackTypes) > 0 {
			fi.FieldTrackTypes = fieldTrackTypes
		}

		rv = append(rv, &CondFlexIndex{
			// TODO: setting NEQ []string to nil as we don't allow a type mapping (enabled
			//       or disabled) along side an enabled default mapping
			Cond:      MakeCondFuncNeqVals(typeFieldPath, nil),
			FlexIndex: fi,
		})
	}

	return rv, nil
}

// ------------------------------------------------------------------------

// Populates into fieldTrackTypeCounts the counts of field types, and into
// fieldTrackTypes all the type mappings within which the field tracks occur.
func countFieldTrackTypes(path []string, typeMappingName string,
	dm *mapping.DocumentMapping, defaultAnalyzer string,
	fieldTrackTypeCounts map[FieldTrack]map[string]int,
	fieldTrackTypes map[FieldTrack]map[string]struct{}) {
	if dm == nil || !dm.Enabled {
		return
	}

	if dm.DefaultAnalyzer != "" {
		defaultAnalyzer = dm.DefaultAnalyzer
	}

	for _, f := range dm.Fields {
		// For now, only consider fields whose propName == f.Name.
		if f.Index && len(path) > 0 && path[len(path)-1] == f.Name {
			if _, ok := BleveSupportedTypes[f.Type]; !ok {
				continue
			}

			analyzer := defaultAnalyzer
			if f.Analyzer != "" {
				analyzer = f.Analyzer
			}

			if f.Type == "text" && analyzer != "keyword" {
				continue
			}

			fieldTrack := FieldTrack(strings.Join(path, "."))

			m := fieldTrackTypeCounts[fieldTrack]
			if m == nil {
				m = map[string]int{}
				fieldTrackTypeCounts[fieldTrack] = m
			}
			m[f.Type] = m[f.Type] + 1

			if len(typeMappingName) > 0 {
				if _, exists := fieldTrackTypes[fieldTrack]; !exists {
					fieldTrackTypes[fieldTrack] = map[string]struct{}{}
				}
				fieldTrackTypes[fieldTrack][typeMappingName] = struct{}{}
			}
		}
	}

	for propName, propDM := range dm.Properties {
		countFieldTrackTypes(append(path, propName), typeMappingName, propDM,
			defaultAnalyzer, fieldTrackTypeCounts, fieldTrackTypes)
	}
}

// ------------------------------------------------------------------------

// This map contains types that Bleve supports for N1QL queries.
var BleveSupportedTypes = map[string]bool{
	"text":     true,
	"number":   true,
	"boolean":  true,
	"datetime": true,
}

// This map translates Bleve's supported types to types as identified
// by N1QL.
var BleveTypeConv = map[string]string{
	"text":     "string",
	"number":   "number",
	"boolean":  "boolean",
	"datetime": "string",
	"string":   "string",
}

// ------------------------------------------------------------------------

// Recursively initializes a FlexIndex from a given bleve document
// mapping.  Note: the backing array for path is mutated as the
// recursion proceeds.
func BleveToFlexIndex(fi *FlexIndex, path []string, dm *mapping.DocumentMapping,
	defaultAnalyzer string, fieldTrackTypeCounts map[FieldTrack]map[string]int) (
	rv *FlexIndex, err error) {
	if dm == nil || !dm.Enabled {
		return fi, nil
	}

	if dm.DefaultAnalyzer != "" {
		defaultAnalyzer = dm.DefaultAnalyzer
	}

	for _, f := range dm.Fields {
		if !f.Index || len(path) <= 0 || path[len(path)-1] != f.Name {
			continue
		}

		if _, ok := BleveSupportedTypes[f.Type]; !ok {
			continue
		}

		analyzer := defaultAnalyzer
		if f.Analyzer != "" {
			analyzer = f.Analyzer
		}

		// For now, only keyword text fields are supported.
		if f.Type == "text" && analyzer != "keyword" {
			continue
		}

		// Fields that are indexed using different types are not supported.
		if len(fieldTrackTypeCounts[FieldTrack(strings.Join(path, "."))]) != 1 {
			continue
		}

		fieldPath := append([]string(nil), path...) // Copy.

		fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
			FieldPath: fieldPath,
			FieldType: f.Type,
		})

		if f.DocValues || f.Store {
			fi.SortableFields = append(fi.SortableFields, strings.Join(fieldPath, "."))
		}

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:       "eq like",
			FieldPath: fieldPath,
			ValueType: f.Type,
		})

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:            "lt gt le ge",
			FieldPath:      fieldPath,
			ValueType:      f.Type,
			FieldTypeCheck: true,
		})

		// TODO: Currently supports only keyword fields.
		// TODO: Need to support geopoint field types?
		// TODO: Need to support non-keyword analyzers?
		// TODO: f.Store IncludeTermVectors, IncludeInAll, DateFormat, DocValues
	}

	ns := make([]string, 0, len(dm.Properties))
	for n := range dm.Properties {
		if n != "" {
			ns = append(ns, n)
		}
	}

	sort.Strings(ns)

	for _, n := range ns {
		fi, err = BleveToFlexIndex(fi, append(path, n), dm.Properties[n],
			defaultAnalyzer, fieldTrackTypeCounts)
		if err != nil {
			return nil, err
		}
	}

	// Support dynamic indexing only when defaultAnalyzer is set to "keyword"
	if dm.Dynamic {
		if defaultAnalyzer == "keyword" {
			dynamicPath := append([]string(nil), path...) // Copy.

			// Register the dynamic path prefix into the indexed
			// fields so complex expressions will be not-sargable.
			fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
				FieldPath: dynamicPath,
				FieldType: "text",
			})
			fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
				FieldPath: dynamicPath,
				FieldType: "number",
			})
			fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
				FieldPath: dynamicPath,
				FieldType: "boolean",
			})
			fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
				FieldPath: dynamicPath,
				FieldType: "datetime",
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "eq like",
				FieldPath:        dynamicPath,
				ValueType:        "text",
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "eq",
				FieldPath:        dynamicPath,
				ValueType:        "number",
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "eq",
				FieldPath:        dynamicPath,
				ValueType:        "boolean",
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "eq",
				FieldPath:        dynamicPath,
				ValueType:        "datetime",
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "lt gt le ge",
				FieldPath:        dynamicPath,
				ValueType:        "text",
				FieldTypeCheck:   true,
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "lt gt le ge",
				FieldPath:        dynamicPath,
				ValueType:        "number",
				FieldTypeCheck:   true,
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "lt gt le ge",
				FieldPath:        dynamicPath,
				ValueType:        "datetime",
				FieldTypeCheck:   true,
				FieldPathPartial: true,
			})

			fi.Dynamic = true
		}
	}

	return fi, nil
}

// ------------------------------------------------------------------------

func wildcardReplacer(s string) string {
	switch s {
	case `\_`:
		return "_"
	case `\%`:
		return "%"
	case `_`:
		return "?"
	case `%`:
		return "*"
	default:
		return s
	}
}

var repl = regexp.MustCompile(`\\_|\\%|_|%`)

// ------------------------------------------------------------------------

// FlexBuildToBleveQuery translates a flex build tree into a bleve
// query tree in map[string]interface{} representation.
func FlexBuildToBleveQuery(fb *FlexBuild, prevSibling map[string]interface{}) (
	q map[string]interface{}, err error) {
	if fb == nil {
		return nil, nil
	}

	isConjunct := fb.Kind == "conjunct"
	if isConjunct || fb.Kind == "disjunct" {
		qs := make([]interface{}, 0, len(fb.Children))

		var prev map[string]interface{}

		for _, c := range fb.Children {
			q, err = FlexBuildToBleveQuery(c, prev)
			if err != nil {
				return nil, err
			}

			if q != nil {
				qs = append(qs, q)

				if isConjunct {
					prev = q // Prev sibling optimization only when conjunct.
				}
			}
		}

		if len(qs) <= 0 {
			return nil, nil // Optimize case of con/disjuncts empty.
		}

		if m, ok := qs[0].(map[string]interface{}); ok && len(qs) == 1 {
			return m, nil // Optimize case of con/disjuncts of 1.
		}

		if isConjunct {
			return map[string]interface{}{"conjuncts": qs}, nil
		}

		return map[string]interface{}{"disjuncts": qs}, nil
	}

	if fb.Kind == "cmpFieldConstant" {
		// Ex: fb.Data: {"eq", "city", "text", `"nyc"`}.
		if args, ok := fb.Data.([]string); ok && len(args) == 4 {
			if args[2] == "text" {
				var v string
				if err = json.Unmarshal([]byte(args[3]), &v); err != nil {
					return nil, err
				}

				switch args[0] {
				case "eq":
					return map[string]interface{}{"term": v, "field": args[1]}, nil
				case "like":
					v = regexp.QuoteMeta(v)
					v = repl.ReplaceAllStringFunc(v, wildcardReplacer)
					return map[string]interface{}{"wildcard": v, "field": args[1]}, nil
				case "lt":
					return MaxTermRangeQuery(args[1], v, false, prevSibling)
				case "le":
					return MaxTermRangeQuery(args[1], v, true, prevSibling)
				case "gt":
					return MinTermRangeQuery(args[1], v, false, prevSibling)
				case "ge":
					return MinTermRangeQuery(args[1], v, true, prevSibling)
				default:
					return nil, fmt.Errorf("incorrect expression: %v", args)
				}
			}

			if args[2] == "number" {
				// Negative numbers will be enclosed within parantheses, so
				// drop any parantheses from the string.
				// For eg. (-10) -> -10
				numStr := strings.Replace(strings.Replace(args[3], "(", "", 1), ")", "", 1)
				var v float64
				if err := json.Unmarshal([]byte(numStr), &v); err != nil {
					return nil, err
				}

				switch args[0] {
				case "eq":
					return map[string]interface{}{
						"min": v, "max": v,
						"inclusive_min": true, "inclusive_max": true,
						"field": args[1],
					}, nil

				case "lt":
					return MaxNumericRangeQuery(args[1], v, false, prevSibling)
				case "le":
					return MaxNumericRangeQuery(args[1], v, true, prevSibling)
				case "gt":
					return MinNumericRangeQuery(args[1], v, false, prevSibling)
				case "ge":
					return MinNumericRangeQuery(args[1], v, true, prevSibling)
				default:
					return nil, fmt.Errorf("incorrect expression: %v", args)
				}
			}

			if args[2] == "boolean" {
				var v bool
				if err := json.Unmarshal([]byte(args[3]), &v); err != nil {
					return nil, err
				}

				if args[0] != "eq" {
					return nil, fmt.Errorf("incorrect expression: %v", args)
				}

				return map[string]interface{}{"bool": v, "field": args[1]}, nil
			}

			if args[2] == "datetime" {
				var v string
				if err = json.Unmarshal([]byte(args[3]), &v); err != nil {
					return nil, err
				}

				// datetime needs to comply with ISO-8601 standard
				if _, _, err := expression.StrToTimeFormat(v); err != nil {
					return nil, err
				}

				switch args[0] {
				case "eq":
					return map[string]interface{}{
						"start": v, "end": v,
						"inclusive_start": true, "inclusive_end": true,
						"field": args[1],
					}, nil

				case "lt":
					return MaxDatetimeRangeQuery(args[1], v, false, prevSibling)
				case "le":
					return MaxDatetimeRangeQuery(args[1], v, true, prevSibling)
				case "gt":
					return MinDatetimeRangeQuery(args[1], v, false, prevSibling)
				case "ge":
					return MinDatetimeRangeQuery(args[1], v, true, prevSibling)
				default:
					return nil, fmt.Errorf("incorrect expression: %v", args)
				}
			}
		}
	} else if fb.Kind == "searchQuery" {
		if data, ok := fb.Data.(map[string]interface{}); ok {
			return data, nil
		}
		return nil, fmt.Errorf("incorrect expression: %v", fb.Data)
	}

	return nil, fmt.Errorf("FlexBuildToBleveQuery: could not convert: %+v", fb)
}

func MinTermRangeQuery(f string, v string, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		_, prevMinOk := prev["min"].(string)
		prevMax, prevMaxOk := prev["max"].(string)
		if !prevMinOk && prevMaxOk && v <= prevMax {
			prev["min"] = v
			prev["inclusive_min"] = inclusive
			return nil, nil
		}
	}

	return map[string]interface{}{
		"min": v, "inclusive_min": inclusive, "field": f,
	}, nil
}

func MaxTermRangeQuery(f string, v string, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		_, prevMaxOk := prev["max"].(string)
		prevMin, prevMinOk := prev["min"].(string)
		if !prevMaxOk && prevMinOk && prevMin <= v {
			prev["max"] = v
			prev["inclusive_max"] = inclusive
			return nil, nil
		}
	}

	return map[string]interface{}{
		"max": v, "inclusive_max": inclusive, "field": f,
	}, nil
}

func MinNumericRangeQuery(f string, v float64, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		_, prevMinOk := prev["min"].(float64)
		prevMax, prevMaxOk := prev["max"].(float64)
		if !prevMinOk && prevMaxOk && v <= prevMax {
			prev["min"] = v
			prev["inclusive_min"] = inclusive
			return nil, nil
		}
	}

	return map[string]interface{}{
		"min": v, "inclusive_min": inclusive, "field": f,
	}, nil
}

func MaxNumericRangeQuery(f string, v float64, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		_, prevMaxOk := prev["max"].(float64)
		prevMin, prevMinOk := prev["min"].(float64)
		if !prevMaxOk && prevMinOk && prevMin <= v {
			prev["max"] = v
			prev["inclusive_max"] = inclusive
			return nil, nil
		}
	}

	return map[string]interface{}{
		"max": v, "inclusive_max": inclusive, "field": f,
	}, nil
}

func MinDatetimeRangeQuery(f string, v string, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		vDT, _, err := expression.StrToTimeFormat(v)
		if err != nil {
			return nil, err
		}
		_, prevStartOk := prev["start"].(string)
		prevEnd, prevEndOk := prev["end"].(string)
		if !prevStartOk && prevEndOk {
			prevEndDT, _, err := expression.StrToTimeFormat(prevEnd)
			if err == nil && (vDT.Before(prevEndDT) || vDT.Equal(prevEndDT)) {
				prev["start"] = v
				prev["inclusive_start"] = inclusive
				return nil, nil
			}
		}
	}

	return map[string]interface{}{
		"start": v, "inclusive_start": inclusive, "field": f,
	}, nil
}

func MaxDatetimeRangeQuery(f string, v string, inclusive bool,
	prev map[string]interface{}) (map[string]interface{}, error) {
	if prev != nil && prev["field"] == f {
		vDT, _, err := expression.StrToTimeFormat(v)
		if err != nil {
			return nil, err
		}
		_, prevEndOk := prev["end"].(string)
		prevStart, prevStartOk := prev["start"].(string)
		if !prevEndOk && prevStartOk {
			prevStartDT, _, err := expression.StrToTimeFormat(prevStart)
			if err == nil && (vDT.After(prevStartDT) || vDT.Equal(prevStartDT)) {
				prev["end"] = v
				prev["inclusive_end"] = inclusive
				return nil, nil
			}
		}
	}

	return map[string]interface{}{
		"end": v, "inclusive_end": inclusive, "field": f,
	}, nil
}
