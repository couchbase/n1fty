//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package verify

import (
	"encoding/json"
	"reflect"
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

		mOut := OptimizeIndexMapping(m, "", "", searchFields)

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

func TestMB52263_Optimize(t *testing.T) {
	customMapping := []byte(`{
		"analysis": {
			"analyzers": {
				"custom_analyzer": {
					"token_filters": [
					"custom_token_filter",
					"to_lower"
					],
					"tokenizer": "unicode",
					"type": "custom"
				}
			},
			"token_filters": {
				"custom_token_filter": {
					"stop_token_map": "custom_stop_words",
					"type": "stop_tokens"
				}
			},
			"token_maps": {
				"custom_stop_words": {
					"tokens": [
					"the"
					],
					"type": "custom"
				}
			}
		},
		"default_analyzer": "custom_analyzer",
		"default_datetime_parser": "dateTimeOptional",
		"default_field": "_all",
		"default_mapping": {
			"dynamic": true,
			"enabled": true,
			"properties": {
				"payload": {
					"default_analyzer": "custom_analyzer",
					"enabled": true,
					"properties": {
						"product": {
							"default_analyzer": "custom_analyzer",
							"enabled": true,
							"properties": {
								"suffix": {
									"enabled": true,
									"fields": [
									{
										"analyzer": "custom_analyzer",
										"index": true,
										"name": "suffix",
										"type": "text"
									}
									]
								}
							}
						}
					}
				}
			}
		},
		"index_dynamic": true,
		"type_field": "_type"
	}`)

	var m *mapping.IndexMappingImpl
	err := json.Unmarshal([]byte(customMapping), &m)
	if err != nil {
		t.Fatal(err)
	}

	searchFields := map[util.SearchField]struct{}{
		{Name: "payload.product.suffix", Type: "text"}: struct{}{},
	}

	optimized := OptimizeIndexMapping(m, "_default", "_default", searchFields)
	optimizedImpl, ok := optimized.(*mapping.IndexMappingImpl)
	if !ok {
		t.Fatal("unexpected error")
	}

	if !reflect.DeepEqual(m.CustomAnalysis, optimizedImpl.CustomAnalysis) {
		t.Fatal("Custom analysis components lost")
	}
}
