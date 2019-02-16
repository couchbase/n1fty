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
	"fmt"

	"github.com/blevesearch/bleve/mapping"
)

// BleveToFlexIndex creates a FlexIndex from a bleve index mapping.
func BleveToFlexIndex(im *mapping.IndexMappingImpl) (*FlexIndex, error) {
	if im.DefaultMapping == nil || len(im.TypeMapping) > 0 {
		return nil, fmt.Errorf("BleveToFlexIndex: currently only supports default mapping")
	}

	fi := &FlexIndex{}

	err := fi.init(im, nil, nil, im.DefaultMapping)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

// Recursively initializes a FlexIndex from a given bleve document
// mapping.  Note: the backing arrays for parents & path are volatile
// as the recursion proceeds.
func (fi *FlexIndex) init(im *mapping.IndexMappingImpl,
	parents []*mapping.DocumentMapping, path []string,
	dm *mapping.DocumentMapping) error {
	if !dm.Enabled {
		return nil
	}

	lineage := append(parents, dm)

	for _, f := range dm.Fields {
		if !f.Index || f.Type != "text" {
			continue
		}

		analyzer := findAnalyzer(im, lineage, f.Analyzer)
		if analyzer != "keyword" {
			continue
		}

		fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
			// Make own copy of volatile path.
			FieldPath: append(append([]string(nil), path...), f.Name),
			FieldType: "string",
		})

		// TODO: Currently support only default mapping.
		// TODO: Currently support only keyword fields.
		// TODO: numeric & datetime field types.
		// TODO: f.Store IncludeTermVectors, IncludeInAll, DateFormat, DocValues
		// TODO: f.SupportedExprs
	}

	for pName, p := range dm.Properties {
		if pName != "" {
			err := fi.init(im, append(parents, dm), append(path, pName), p)
			if err != nil {
				return err
			}
		}
	}

	if dm.Dynamic {
		// Support dynamic fields only when default datetime parser is
		// disabled, otherwise the usual "dateTimeOptional" parser on
		// a dynamic field will covert text strings that parse as a
		// date into datetime representation.
		if im.DefaultDateTimeParser == "disabled" {
			analyzer := findAnalyzer(im, lineage, "")
			if analyzer == "keyword" {
				fi.IndexedFields = append(fi.IndexedFields, &FieldInfo{
					FieldPath: append([]string(nil), path...), // Make own copy.
					FieldType: "string",
				})
			}
		}
	}

	return nil
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
