//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package verify

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"

	"github.com/couchbase/n1fty/util"
)

func TestOptimizeIndexMapping(t *testing.T) {
	tests := []struct {
		about         string
		mapping       string
		searchFields  string // semicolon separated "name(,type(,analyzer))" strings.
		mappingExpect string
	}{
		{
			about: "searching for non-indexed fields",
			mapping: `{
				"default_analyzer": "standard",
				"default_mapping": {
					"dynamic": false,
					"enabled": false
				},
				"types": {
					"hotel": {
						"enabled": true,
						"dynamic": false,
						"default_analyzer": "cjk",
						"properties": {
							"country": {
								"enabled": true,
								"dynamic": false,
								"fields": [{
									"name": "country",
									"type": "text",
									"analyzer": "da",
									"index": true
								}]
							}
						}
					}
				}
			}`,
			searchFields: "foo;bar",
			mappingExpect: `{
				"default_mapping": {
					"enabled": false,
					"dynamic": false
				},
				"type_field": "_type",
				"default_type": "_default",
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"store_dynamic": true,
				"index_dynamic": true,
				"docvalues_dynamic": true,
				"analysis": {}
			}`,
		},
		{
			about: "searching for an index field",
			mapping: `{
				"default_analyzer": "standard",
				"default_mapping": {
					"dynamic": false,
					"enabled": false
				},
				"types": {
					"hotel": {
						"enabled": true,
						"dynamic": false,
						"default_analyzer": "cjk",
						"properties": {
							"country": {
								"enabled": true,
								"dynamic": false,
								"fields": [{
									"name": "country",
									"type": "text",
									"analyzer": "da",
									"index": true
								}]
							},
							"state": {
								"enabled": true,
								"dynamic": false,
								"fields": [{
									"name": "state",
									"type": "text",
									"analyzer": "da",
									"index": true
								}]
							},
							"city": {
								"enabled": true,
								"dynamic": false,
								"fields": [{
									"name": "city",
									"type": "text",
									"analyzer": "da",
									"index": true
								}]
							}
						}
					}
				}
			}`,
			searchFields: "country,text,da",
			mappingExpect: `{
				"types": {
					"hotel": {
						"enabled": true,
						"dynamic": false,
						"properties": {
							"country": {
								"enabled": true,
								"dynamic": false,
								"fields": [
								{
									"name": "country",
									"type": "text",
									"analyzer": "da",
									"index": true
								}
								]
							}
						},
						"default_analyzer": "cjk"
					}
				},
				"default_mapping": {
					"enabled": false,
					"dynamic": false
				},
				"type_field": "_type",
				"default_type": "_default",
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"store_dynamic": true,
				"index_dynamic": true,
				"docvalues_dynamic": true,
				"analysis": {}
			}`,
		},
		{
			about: "searching for a nested index field",
			mapping: `{
				"default_analyzer": "standard",
				"default_mapping": {
					"dynamic": false,
					"enabled": false
				},
				"types": {
					"hotel": {
						"enabled": true,
						"dynamic": false,
						"default_analyzer": "cjk",
						"properties": {
							"address": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "country",
											"type": "text",
											"index": true
										}]
									},
									"state": {
										"enabled": true,
										"dynamic": false,
										"fields": [{
											"name": "state",
											"type": "text",
											"index": true
										}]
									}
								}
							}
						}
					}
				}
			}`,
			searchFields: "foo;address.country,text,cjk",
			mappingExpect: `{
				"types": {
					"hotel": {
						"enabled": true,
						"dynamic": false,
						"properties": {
							"address": {
								"enabled": true,
								"dynamic": false,
								"properties": {
									"country": {
										"enabled": true,
										"dynamic": false,
										"fields": [
										{
											"name": "country",
											"type": "text",
											"index": true
										}
										]
									}
								}
							}
						},
						"default_analyzer": "cjk"
					}
				},
				"default_mapping": {
					"enabled": false,
					"dynamic": false
				},
				"type_field": "_type",
				"default_type": "_default",
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"store_dynamic": true,
				"index_dynamic": true,
				"docvalues_dynamic": true,
				"analysis": {}
			}`,
		},
	}

	for testi, test := range tests {
		var m *mapping.IndexMappingImpl
		err := json.Unmarshal([]byte(test.mapping), &m)
		if err != nil {
			t.Fatal(err)
		}

		searchFields := map[util.SearchField]struct{}{}

		for _, s := range strings.Split(test.searchFields, ";") {
			nta := strings.Split(s, ",") // "name,type,analyzer".

			sf := util.SearchField{Name: nta[0]}
			if len(nta) >= 2 {
				sf.Type = nta[1]
			}
			if len(nta) >= 3 {
				sf.Analyzer = nta[2]
			}

			searchFields[sf] = struct{}{}
		}

		mOut := OptimizeIndexMapping(m, searchFields)

		var mExpect *mapping.IndexMappingImpl
		err = json.Unmarshal([]byte(test.mappingExpect), &mExpect)
		if err != nil {
			t.Fatal(err)
		}

		jOut, _ := json.Marshal(mOut)
		jExpect, _ := json.Marshal(mExpect)

		if string(jOut) != string(jExpect) {
			t.Fatalf("testi: %d, test: %+v,\n mismatch mOut, got:\n   %s\n expect:\n   %s",
				testi, test, jOut, jExpect)
		}
	}
}
