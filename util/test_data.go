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

package util

// Note only the landmark type mapping is enabled, as n1fty currently
// only supports a single enabled type mapping.
var SampleLandmarkIndexDef = []byte(`
{
	"name": "temp",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "standard",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": true,
				"enabled": false
			},
			"default_type": "_default",
			"docvalues_dynamic": true,
			"index_dynamic": true,
			"store_dynamic": false,
			"type_field": "_type",
			"types": {
				"hotel": {
					"enabled": false,
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
								"store": false,
								"index": true,
								"include_term_vectors": true,
								"include_in_all": true,
								"docvalues": true
							}]
						}
					}
				},
				"landmark": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"reviews": {
							"enabled": true,
							"dynamic": false,
							"properties": {
								"review": {
									"enabled": true,
									"dynamic": true,
									"properties": {
										"author": {
											"enabled": true,
											"dynamic": false,
											"fields": [{
												"name": "author",
												"type": "text",
												"store": true,
												"index": true,
												"include_term_vectors": true,
												"include_in_all": true,
												"analyzer": "de",
												"docvalues": true
											}]
										}
									}
								},
								"id": {
									"enabled": true,
									"dynamic": false,
									"fields": [{
										"name": "id",
										"type": "text",
										"store": false,
										"index": true,
										"include_term_vectors": true,
										"include_in_all": true,
										"docvalues": true
									}]
								}
							}
						},
						"country": {
							"enabled": true,
							"dynamic": false,
							"fields": [{
								"docvalues": true,
								"include_in_all": true,
								"include_term_vectors": true,
								"index": true,
								"name": "countryX",
								"store": true,
								"type": "text"
							}]
						}
					}
				}
			}
		},
		"store": {
			"indexType": "scorch",
			"kvStoreName": ""
		}
	},
	"sourceType": "couchbase",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"numReplicas": 0
	},
	"uuid": ""
}
`)

var SampleIndexDefDynamicDefault = []byte(`
{
	"name": "temp",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "standard",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": true,
				"enabled": true
			},
			"default_type": "_default",
			"docvalues_dynamic": true,
			"index_dynamic": true,
			"store_dynamic": false,
			"type_field": "_type"
		},
		"store": {
			"indexType": "scorch",
			"kvStoreName": ""
		}
	},
	"sourceType": "couchbase",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"numReplicas": 0
	},
	"uuid": ""
}
`)

var SampleIndexDefWithAnalyzerEN = []byte(`
{
	"name": "temp",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "en",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": true,
				"enabled": true
			},
			"default_type": "_default",
			"docvalues_dynamic": true,
			"index_dynamic": true,
			"store_dynamic": false,
			"type_field": "_type"
		},
		"store": {
			"indexType": "scorch",
			"kvStoreName": ""
		}
	},
	"sourceType": "couchbase",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"numReplicas": 0
	},
	"uuid": ""
}
`)

var SampleIndexDefWithCustomDefaultMapping = []byte(`
{
	"name": "travel",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "standard",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": false,
				"enabled": true,
				"properties": {
					"city": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "city",
							"type": "text"
						}
						]
					},
					"country": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "country",
							"type": "text"
						}
						]
					},
					"currentTime": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "currentTime",
							"type": "datetime"
						}
						]
					}
				}
			},
			"default_type": "_default",
			"docvalues_dynamic": false,
			"index_dynamic": false,
			"store_dynamic": false,
			"type_field": "_type"
		},
		"store": {
			"indexType": "scorch",
			"kvStoreName": ""
		}
	},
	"sourceType": "couchbase",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"numReplicas": 0
	},
	"uuid": ""
}
`)
