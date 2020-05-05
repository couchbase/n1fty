//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package flex

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/mapping"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var DefaultTypeFieldPath = []string{"type"}

// BleveToCondFlexIndexes translates a bleve index into CondFlexIndexes.
// NOTE: checking for DocConfig.Mode should be done beforehand.
func BleveToCondFlexIndexes(im *mapping.IndexMappingImpl,
	typeFieldPath []string) (rv CondFlexIndexes, err error) {
	// Map of FieldTrack => fieldType => count.
	fieldTrackTypes := map[FieldTrack]map[string]int{}
	for _, dm := range im.TypeMapping {
		countFieldTrackTypes(nil, dm, im.DefaultAnalyzer, fieldTrackTypes)
	}
	countFieldTrackTypes(nil, im.DefaultMapping, im.DefaultAnalyzer, fieldTrackTypes)

	types := make([]string, 0, len(im.TypeMapping))
	for t := range im.TypeMapping {
		types = append(types, t)
	}

	sort.Strings(types) // For output stability.

	if len(typeFieldPath) == 0 {
		typeFieldPath = DefaultTypeFieldPath
	}

	for _, t := range types {
		typeEqEffect := "FlexBuild:n" // Strips `type = "BEER"` from expressions.
		if !im.TypeMapping[t].Enabled {
			typeEqEffect = "not-sargable"
		}

		fi, err := BleveToFlexIndex(&FlexIndex{
			// To lead CheckFieldsUseds() to not-sargable.
			IndexedFields: FieldInfos{
				&FieldInfo{FieldPath: typeFieldPath, FieldType: "text"},
			},
			SupportedExprs: []SupportedExpr{
				// Strips `type = "BEER"` from expressions.
				&SupportedExprCmpFieldConstant{
					Cmp:       "eq",
					FieldPath: typeFieldPath,
					ValueType: "text",
					ValueMust: value.NewValue(t),
					Effect:    typeEqEffect,
				},
				// To treat `type > "BEER"` as not-sargable.
				&SupportedExprCmpFieldConstant{
					Cmp:       "lt gt le ge",
					FieldPath: typeFieldPath,
					ValueType: "", // Treated as not-sargable.
					Effect:    "not-sargable",
				},
			},
		}, im, nil, im.TypeMapping[t], im.DefaultAnalyzer, fieldTrackTypes)
		if err != nil {
			return nil, err
		}

		rv = append(rv, &CondFlexIndex{
			Cond:      MakeCondFuncEqVal(typeFieldPath, value.NewValue(t)),
			FlexIndex: fi,
		})
	}

	if im.DefaultMapping != nil {
		fi, err := BleveToFlexIndex(&FlexIndex{
			IndexedFields: FieldInfos{},
		}, im, nil, im.DefaultMapping, im.DefaultAnalyzer, fieldTrackTypes)
		if err != nil {
			return nil, err
		}

		rv = append(rv, &CondFlexIndex{
			Cond:      MakeCondFuncNeqVals(typeFieldPath, types),
			FlexIndex: fi,
		})
	}

	return rv, nil
}

// ------------------------------------------------------------------------

// Populates into mm the counts of field types.
func countFieldTrackTypes(path []string, dm *mapping.DocumentMapping,
	defaultAnalyzer string, mm map[FieldTrack]map[string]int) {
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

			m := mm[fieldTrack]
			if m == nil {
				m = map[string]int{}
				mm[fieldTrack] = m
			}

			m[f.Type] = m[f.Type] + 1
		}
	}

	for propName, propDM := range dm.Properties {
		countFieldTrackTypes(append(path, propName), propDM, defaultAnalyzer, mm)
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
func BleveToFlexIndex(fi *FlexIndex, im *mapping.IndexMappingImpl,
	path []string, dm *mapping.DocumentMapping, defaultAnalyzer string,
	fieldTrackTypes map[FieldTrack]map[string]int) (rv *FlexIndex, err error) {
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
		if len(fieldTrackTypes[FieldTrack(strings.Join(path, "."))]) != 1 {
			continue
		}

		fieldPath := append([]string(nil), path...) // Copy.

		fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
			FieldPath: fieldPath,
			FieldType: f.Type,
		})

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:       "eq",
			FieldPath: fieldPath,
			ValueType: f.Type,
		})

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:            "lt gt le ge",
			FieldPath:      fieldPath,
			ValueType:      f.Type,
			FieldTypeCheck: true,
		})

		// TODO: Currently supports only default mapping.
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
		fi, err = BleveToFlexIndex(fi, im,
			append(path, n), dm.Properties[n], defaultAnalyzer, fieldTrackTypes)
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
				Cmp:              "eq",
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

// --------------------------------------

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
