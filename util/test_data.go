//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package util

// Note only the landmark type mapping is enabled, as n1fty currently
// only supports a single enabled type mapping.
var SampleLandmarkIndexDef = []byte(`
{
	"name": "SampleLandmarkIndexDef",
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
	"name": "SampleIndexDefDynamicDefault",
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

var SampleIndexDefDynamicWithAnalyzerKeyword = []byte(`
{
	"name": "SampleIndexDefDynamicWithAnalyzerKeyword",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "keyword",
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
	"name": "SampleIndexDefWithCustomDefaultMapping",
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
							"type": "text",
							"analyzer": "keyword"
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

var SampleIndexDefWithNoAllField = []byte(`
{
	"name": "SampleIndexDefWithNoAllField",
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
							"include_term_vectors": true,
							"index": true,
							"name": "city",
							"type": "text"
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

var SampleIndexDefWithKeywordAnalyzerOverDefaultMapping = []byte(`
{
	"name": "SampleIndexDefWithKeywordAnalyzerOverDefaultMapping",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "keyword",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": false,
				"enabled": true,
				"properties": {
					"type": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "type",
							"type": "text"
						}
						]
					},
					"id": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"name": "id",
							"type": "number",
							"index": true,
							"include_in_all": true,
							"docvalues": true
						}
						]
					},
					"isOpen": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"name": "isOpen",
							"type": "boolean",
							"index": true,
							"include_in_all": true,
							"docvalues": true
						}
						]
					},
					"createdOn": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"name": "createdOn",
							"type": "datetime",
							"index": true,
							"include_in_all": true,
							"docvalues": true
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

var SampleIndexDefWithSeveralNestedFieldsUnderDefaultMapping = []byte(`
{
	"name": "SampleIndexDefWithSeveralNestedFieldsUnderDefaultMapping",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "keyword",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": false,
				"enabled": true,
				"properties": {
					"reviews": {
						"dynamic": false,
						"enabled": true,
						"properties": {
							"ratings": {
								"dynamic": false,
								"enabled": true,
								"properties": {
									"Cleanliness": {
										"enabled": true,
										"dynamic": false,
										"fields": [
										{
											"docvalues": true,
											"include_in_all": true,
											"include_term_vectors": true,
											"index": true,
											"name": "Cleanliness",
											"type": "number"
										}
										]
									},
									"Overall": {
										"enabled": true,
										"dynamic": false,
										"fields": [
										{
											"docvalues": true,
											"include_in_all": true,
											"include_term_vectors": true,
											"index": true,
											"name": "Overall",
											"type": "number"
										}
										]
									}
								}
							},
							"author": {
								"enabled": true,
								"dynamic": false,
								"fields": [
								{
									"docvalues": true,
									"include_in_all": true,
									"include_term_vectors": true,
									"index": true,
									"name": "author",
									"type": "text"
								}
								]
							}
						}
					},
					"public_likes": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "public_likes",
							"type": "text"
						}
						]
					},
					"type": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"docvalues": true,
							"include_in_all": true,
							"include_term_vectors": true,
							"index": true,
							"name": "type",
							"type": "text"
						}
						]
					}
				}
			},
			"default_type": "_default",
			"docvalues_dynamic": true,
			"index_dynamic": true,
			"store_dynamic": false,
			"type_field": "_type"
		},
		"store": {
			"indexType": "scorch"
		}
	},
	"sourceType": "gocbcore",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"indexPartitions": 6,
		"numReplicas": 0
	},
	"uuid": ""
}
`)

var SampleIndexDefWithSeveralNestedFieldsUnderHotelMapping = []byte(`
{
	"name": "SampleIndexDefWithSeveralNestedFieldsUnderHotelMapping",
	"type": "fulltext-index",
	"params": {
		"doc_config": {
			"docid_prefix_delim": "",
			"docid_regexp": "",
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"default_analyzer": "keyword",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": false,
				"enabled": false
			},
			"default_type": "_default",
			"docvalues_dynamic": true,
			"index_dynamic": true,
			"store_dynamic": false,
			"type_field": "_type",
			"types": {
				"hotel": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"reviews": {
							"dynamic": false,
							"enabled": true,
							"properties": {
								"ratings": {
									"dynamic": false,
									"enabled": true,
									"properties": {
										"Cleanliness": {
											"enabled": true,
											"dynamic": false,
											"fields": [
											{
												"docvalues": true,
												"include_in_all": true,
												"include_term_vectors": true,
												"index": true,
												"name": "Cleanliness",
												"type": "number"
											}
											]
										},
										"Overall": {
											"enabled": true,
											"dynamic": false,
											"fields": [
											{
												"docvalues": true,
												"include_in_all": true,
												"include_term_vectors": true,
												"index": true,
												"name": "Overall",
												"type": "number"
											}
											]
										}
									}
								},
								"author": {
									"enabled": true,
									"dynamic": false,
									"fields": [
									{
										"docvalues": true,
										"include_in_all": true,
										"include_term_vectors": true,
										"index": true,
										"name": "author",
										"type": "text"
									}
									]
								}
							}
						},
						"public_likes": {
							"enabled": true,
							"dynamic": false,
							"fields": [
							{
								"docvalues": true,
								"include_in_all": true,
								"include_term_vectors": true,
								"index": true,
								"name": "public_likes",
								"type": "text"
							}
							]
						},
						"createdOn": {
							"enabled": true,
							"dynamic": false,
							"fields": [
							{
								"name": "createdOn",
								"type": "datetime",
								"index": true,
								"include_in_all": true,
								"docvalues": true
							}
							]
						}
					}
				}
			}
		},
		"store": {
			"indexType": "scorch"
		}
	},
	"sourceType": "gocbcore",
	"sourceName": "travel-sample",
	"sourceUUID": "",
	"sourceParams": {},
	"planParams": {
		"maxPartitionsPerPIndex": 171,
		"indexPartitions": 6,
		"numReplicas": 0
	},
	"uuid": ""
}
`)

var SampleIndexDefWithMultipleTypeMappings = []byte(`
{
	"type": "fulltext-index",
	"name": "SampleIndexDefWithMultipleTypeMappings",
	"sourceType": "gocbcore",
	"sourceName": "travel-sample",
	"planParams": {
		"indexPartitions": 6
	},
	"params": {
		"doc_config": {
			"mode": "type_field",
			"type_field": "type"
		},
		"mapping": {
			"analysis": {},
			"default_analyzer": "standard",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": true,
				"enabled": false
			},
			"default_type": "_default",
			"type_field": "_type",
			"types": {
				"airline": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"country": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "country",
								"type": "text"
							}
							]
						}
					}
				},
				"airport": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"country": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "country",
								"type": "text"
							}
							]
						},
						"city": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "city",
								"type": "text"
							}
							]
						}
					}
				}
			}
		},
		"store": {
			"indexType": "scorch"
		}
	}
}
`)

var SampleIndexDefWithDocIdPrefixMultipleTypeMappings = []byte(`
{
	"type": "fulltext-index",
	"name": "SampleIndexDefWithDocIDPrefixMultipleTypeMappings",
	"sourceType": "gocbcore",
	"sourceName": "travel-sample",
	"planParams": {
		"indexPartitions": 6
	},
	"params": {
		"doc_config": {
			"mode": "docid_prefix",
			"docid_prefix_delim": "-"
		},
		"mapping": {
			"analysis": {},
			"default_analyzer": "standard",
			"default_datetime_parser": "dateTimeOptional",
			"default_field": "_all",
			"default_mapping": {
				"dynamic": true,
				"enabled": false
			},
			"default_type": "_default",
			"type_field": "_type",
			"types": {
				"airline": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"country": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "country",
								"type": "text"
							}
							]
						}
					}
				},
				"airport": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"country": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "country",
								"type": "text"
							}
							]
						},
						"city": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "keyword",
								"index": true,
								"name": "city",
								"type": "text"
							}
							]
						}
					}
				}
			}
		},
		"store": {
			"indexType": "scorch"
		}
	}
}
`)
