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

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
)

// BleveToFlexIndex creates a FlexIndex from a bleve index mapping.
func BleveToFlexIndex(im *mapping.IndexMappingImpl) (*FlexIndex, error) {
	if im.DefaultMapping == nil || len(im.TypeMapping) > 0 {
		return nil, fmt.Errorf("BleveToFlexIndex: currently only supports default mapping")
	}

	return bleveToFlexIndex(&FlexIndex{}, im, nil, nil, im.DefaultMapping)
}

var BleveTypeConv = map[string]string{"text": "string", "number": "number"}

// Recursively initializes a FlexIndex from a given bleve document
// mapping.  Note: the backing arrays for parents & path are volatile
// as the recursion proceeds.
func bleveToFlexIndex(fi *FlexIndex, im *mapping.IndexMappingImpl,
	parents []*mapping.DocumentMapping, path []string,
	dm *mapping.DocumentMapping) (rv *FlexIndex, err error) {
	if !dm.Enabled {
		return fi, nil
	}

	lineage := append(parents, dm)

	for _, f := range dm.Fields {
		if !f.Index {
			continue
		}

		fieldType, ok := BleveTypeConv[f.Type]
		if !ok {
			continue
		}

		if f.Type == "text" && findAnalyzer(im, lineage, f.Analyzer) != "keyword" {
			continue
		}

		// Make own copy of volatile path.
		fieldPath := append(append([]string(nil), path...), f.Name)

		fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
			FieldPath: fieldPath,
			FieldType: fieldType,
		})

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:       "eq",
			FieldPath: fieldPath,
			ValueType: fieldType,
		})

		fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
			Cmp:            "lt gt le ge",
			FieldPath:      fieldPath,
			ValueType:      fieldType,
			FieldTypeCheck: true,
		})

		// TODO: Currently supports only default mapping.
		// TODO: Currently supports only keyword fields.
		// TODO: Need to support datetime field types?
		// TODO: Need to support geopoint field types?
		// TODO: Need to support bool field types?
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
		fi, err = bleveToFlexIndex(fi, im, lineage, append(path, n), dm.Properties[n])
		if err != nil {
			return nil, err
		}
	}

	// Support dynamic indexing only when default datetime parser
	// is disabled, otherwise the usual "dateTimeOptional" parser
	// on a dynamic field will covert text strings that look like
	// or parse as a date-time into datetime representation.
	if dm.Dynamic && im.DefaultDateTimeParser == "disabled" {
		if findAnalyzer(im, lineage, "") == "keyword" {
			dynamicPath := append([]string(nil), path...) // Copy.

			// Register the dynamic path prefix into the indexed
			// fields so complex expressions will be not-sargable.
			fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
				FieldPath: dynamicPath,
				FieldType: "string",
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "eq",
				FieldPath:        dynamicPath,
				ValueType:        "string",
				FieldPathPartial: true,
			})

			fi.SupportedExprs = append(fi.SupportedExprs, &SupportedExprCmpFieldConstant{
				Cmp:              "lt gt le ge",
				FieldPath:        dynamicPath,
				ValueType:        "string",
				FieldTypeCheck:   true,
				FieldPathPartial: true,
			})

			// TODO: Support dynamic number (and other) types?
		}
	}

	return fi, nil
}

// Walk up the document mappings to find an analyzer name.
func findAnalyzer(im *mapping.IndexMappingImpl,
	lineage []*mapping.DocumentMapping, analyzer string) string {
	if analyzer != "" {
		return analyzer
	}

	for i := len(lineage) - 1; i >= 0; i-- {
		if lineage[i].DefaultAnalyzer != "" {
			return lineage[i].DefaultAnalyzer
		}
	}

	return im.DefaultAnalyzer
}

// --------------------------------------

// FlexBuildToBleveQuery translates a flex build tree into a bleve
// query tree.
func FlexBuildToBleveQuery(fb *FlexBuild, prevSibling query.Query) (
	q query.Query, err error) {
	if fb == nil {
		return nil, nil
	}

	isConjunct := fb.Kind == "conjunct"
	if isConjunct || fb.Kind == "disjunct" {
		qs := make([]query.Query, 0, len(fb.Children))

		var prev query.Query

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

		if isConjunct {
			return query.NewConjunctionQuery(qs), nil
		}

		return query.NewDisjunctionQuery(qs), nil
	}

	if fb.Kind == "cmpFieldConstant" {
		// Ex: fb.Data: {"eq", "city", "string", `"nyc"`}.
		args, ok := fb.Data.([]string)
		if ok && len(args) == 4 {
			if args[2] == "string" {
				var v string
				err := json.Unmarshal([]byte(args[3]), &v)
				if err != nil {
					return nil, err
				}

				switch args[0] {
				case "eq":
					q := query.NewTermQuery(v)
					q.SetField(args[1])
					return q, nil

				case "lt":
					return MaxTermRangeQuery(args[1], v, &falseVal, prevSibling)
				case "le":
					return MaxTermRangeQuery(args[1], v, &trueVal, prevSibling)
				case "gt":
					return MinTermRangeQuery(args[1], v, &falseVal, prevSibling)
				case "ge":
					return MinTermRangeQuery(args[1], v, &trueVal, prevSibling)
				}
			}

			if args[2] == "number" {
				var v float64
				err := json.Unmarshal([]byte(args[3]), &v)
				if err != nil {
					return nil, err
				}

				switch args[0] {
				case "eq":
					q := query.NewNumericRangeInclusiveQuery(&v, &v, &trueVal, &trueVal)
					q.SetField(args[1])
					return q, nil

				case "lt":
					return MaxNumericRangeQuery(args[1], &v, &falseVal, prevSibling)
				case "le":
					return MaxNumericRangeQuery(args[1], &v, &trueVal, prevSibling)
				case "gt":
					return MinNumericRangeQuery(args[1], &v, &falseVal, prevSibling)
				case "ge":
					return MinNumericRangeQuery(args[1], &v, &trueVal, prevSibling)
				}
			}
		}
	}

	return nil, fmt.Errorf("FlexBuildToBleveQuery: could not convert: %+v", fb)
}

var trueVal = true
var falseVal = false

func MinTermRangeQuery(f string, v string, inclusive *bool, prev query.Query) (
	query.Query, error) {
	if trq, ok := prev.(*query.TermRangeQuery); ok &&
		trq != nil && trq.Field() == f && trq.Min == "" && v <= trq.Max {
		trq.Min = v
		trq.InclusiveMin = inclusive
		return nil, nil
	}

	q := query.NewTermRangeInclusiveQuery(v, "", inclusive, &falseVal)
	q.SetField(f)
	return q, nil
}

func MaxTermRangeQuery(f string, v string, inclusive *bool, prev query.Query) (
	query.Query, error) {
	if trq, ok := prev.(*query.TermRangeQuery); ok &&
		trq != nil && trq.Field() == f && trq.Max == "" && trq.Min <= v {
		trq.Max = v
		trq.InclusiveMax = inclusive
		return nil, nil
	}

	q := query.NewTermRangeInclusiveQuery("", v, &falseVal, inclusive)
	q.SetField(f)
	return q, nil
}

func MinNumericRangeQuery(f string, v *float64, inclusive *bool, prev query.Query) (
	query.Query, error) {
	if trq, ok := prev.(*query.NumericRangeQuery); ok &&
		trq != nil && trq.Field() == f && trq.Min == nil && *v <= *trq.Max {
		trq.Min = v
		trq.InclusiveMin = inclusive
		return nil, nil
	}

	q := query.NewNumericRangeInclusiveQuery(v, nil, inclusive, &falseVal)
	q.SetField(f)
	return q, nil
}

func MaxNumericRangeQuery(f string, v *float64, inclusive *bool, prev query.Query) (
	query.Query, error) {
	if trq, ok := prev.(*query.NumericRangeQuery); ok &&
		trq != nil && trq.Field() == f && trq.Max == nil && *trq.Min <= *v {
		trq.Max = v
		trq.InclusiveMax = inclusive
		return nil, nil
	}

	q := query.NewNumericRangeInclusiveQuery(nil, v, &falseVal, inclusive)
	q.SetField(f)
	return q, nil
}
