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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/mapping"
)

func TestBleveToFlexIndex(t *testing.T) {
	tests := []struct {
		m               *mapping.IndexMappingImpl
		expectFlexIndex *FlexIndex
		expectErr       string
	}{
		{
			m:         &mapping.IndexMappingImpl{},
			expectErr: "because there isn't a default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"not-handled": {},
				},
			},
			expectErr: "because there isn't a default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"not-handled": {},
				},
				DefaultMapping: &mapping.DocumentMapping{},
			},
			expectErr: "because there isn't a only default mapping",
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "NOT-text", Analyzer: "keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "NOT-keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: false, /* Index FALSE */
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: true,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "",
							Index: true,
						},
					},
				},
				DefaultAnalyzer: "NOT-keyword",
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", /* Analyzer: "", */
							Index: true,
						},
					},
				},
				DefaultAnalyzer: "keyword",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Fields: []*mapping.FieldMapping{
						{
							Name: "f1", Type: "text", Analyzer: "keyword",
							Index: true,
						},
						{
							Name: "f2", Type: "text", Analyzer: "keyword",
							Index: false,
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"f1"},
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"addr": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"addr", "f1"},
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Properties: map[string]*mapping.DocumentMapping{
						"addr": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
						"pets": {
							Enabled: true,
							Fields: []*mapping.FieldMapping{
								{
									Name: "f1", Type: "text", Analyzer: "keyword",
									Index: true,
								},
							},
						},
					},
				},
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: []string{"addr", "f1"},
						FieldType: "string",
					},
					&FieldInfo{
						FieldPath: []string{"pets", "f1"},
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: true,
				},
			},
			expectFlexIndex: &FlexIndex{},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: true,
					Dynamic: true,
				},
				DefaultAnalyzer:       "keyword",
				DefaultDateTimeParser: "disabled",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: nil,
						FieldType: "string",
					},
				},
			},
		},
		{
			m: &mapping.IndexMappingImpl{
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:         true,
					Dynamic:         true,
					DefaultAnalyzer: "keyword",
				},
				DefaultDateTimeParser: "disabled",
			},
			expectFlexIndex: &FlexIndex{
				IndexedFields: FieldInfos{
					&FieldInfo{
						FieldPath: nil,
						FieldType: "string",
					},
				},
			},
		},
	}

	for _, test := range tests {
		flexIndex, err := BleveToFlexIndex(test.m)
		if (err != nil) != (test.expectErr != "") {
			t.Fatalf("test: %+v,\n err mismatch, got: %v",
				test, err)
		}

		if !reflect.DeepEqual(flexIndex, test.expectFlexIndex) {
			jm, _ := json.Marshal(test.m)
			jefi, _ := json.Marshal(test.expectFlexIndex)
			jfi, _ := json.Marshal(flexIndex)

			t.Errorf("test: %+v,\n jm: %s\n expectFlexIndex: %s\n flexIndex mismatch, got: %s",
				test, jm, jefi, jfi)
		}
	}
}
