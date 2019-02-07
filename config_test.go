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

package n1fty

import (
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/errors"
	"testing"
)

var tconfig n1ftyConfig

func GetTestConfig() (Cfg, errors.Error) {
	return &tconfig, nil
}

func cleanConfig() {
	sampleConf, _ := GetTestConfig()
	conf := make(map[string]interface{})
	conf["/fts/cbgt/cfg/indexDefs"] = nil
	conf["/fts/cbgt/cfg/nodeDefs-known/5859d032c9cd9f1afb62cdf207c7d173"] = []byte(nil)
	conf["/fts/cbgt/cfg/nodeDefs-known/d6ad6468930d9a57c4b7e90af0fb1bff"] = []byte(nil)
	conf["/fts/cbgt/cfg/nodeDefs-known/aa32875f1b85c5d8b3f26a55ba6d251e"] = []byte(nil)
	_ = sampleConf.SetConfig(conf)
}

func TestGetIndexDefs(t *testing.T) {
	sampleIndexDef := []byte(`{"uuid":"3c52d65b00000180",
	"indexDefs":{"FTS":{"type":"fulltext-index","name":"FTS",
	"uuid":"3c52d65b00000180","sourceType":"couchbase",
	"sourceName":"beer-sample","sourceUUID":"77189afd8630ce46fd9a3b8a410fc4b9",
	"planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":
	{"docid_prefix_delim":"","docid_regexp":"","mode":"type_field",
	"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard",
	"default_datetime_parser":"dateTimeOptional","default_field":"_all",
	"default_mapping":{"dynamic":true,"enabled":true},
	"default_type":"_default","docvalues_dynamic":true,"index_dynamic":true,
	"store_dynamic":false,"type_field":"_type"},"store":
	{"indexType":"scorch","kvStoreName":""}},"sourceParams":{}}},
	"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	conf := make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/indexDefs"] = sampleIndexDef
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	var er error
	var indexDefs *cbgt.IndexDefs
	indexDefs, er = GetIndexDefs(sampleConf)
	if er != nil || len(indexDefs.IndexDefs) != 1 {
		t.Errorf("GetIndexDefs, err: %v", er)
	}

	indexDefn := indexDefs.IndexDefs["FTS"]
	if indexDefn == nil || indexDefn.UUID != "3c52d65b00000180" {
		t.Errorf("mismatched indexDef")
	}

	cleanConfig()
}

func TestMultipleGetIndexDefs(t *testing.T) {
	sampleIndexDef := []byte(`{"uuid":"3c52d65b00000180",
	"indexDefs":{"BEER":{"type":"fulltext-index","name":"BEER",
	"uuid":"3c52d65b00000180","sourceType":"couchbase",
	"sourceName":"beer-sample","sourceUUID":"77189afd8630ce46fd9a3b8a410fc4b9",
	"planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":
	{"docid_prefix_delim":"","docid_regexp":"","mode":"type_field",
	"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard",
	"default_datetime_parser":"dateTimeOptional","default_field":"_all",
	"default_mapping":{"dynamic":true,"enabled":true},
	"default_type":"_default","docvalues_dynamic":true,"index_dynamic":true,
	"store_dynamic":false,"type_field":"_type"},"store":
	{"indexType":"scorch","kvStoreName":""}},"sourceParams":{}},
	"TRAVEL":{"type":"fulltext-index","name":"TRAVEL",
	"uuid":"3c52d65b000001801","sourceType":"couchbase",
	"sourceName":"travel-sample","sourceUUID":"77189afd8630ce46fd9a3b8a410fc4b9",
	"planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":
	{"docid_prefix_delim":"","docid_regexp":"","mode":"type_field",
	"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard",
	"default_datetime_parser":"dateTimeOptional","default_field":"_all",
	"default_mapping":{"dynamic":true,"enabled":true},
	"default_type":"_default","docvalues_dynamic":true,"index_dynamic":true,
	"store_dynamic":false,"type_field":"_type"},"store":
	{"indexType":"scorch","kvStoreName":""}},"sourceParams":{}}},
	"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	conf := make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/indexDefs"] = sampleIndexDef
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	var er error
	var indexDefs *cbgt.IndexDefs
	indexDefs, er = GetIndexDefs(sampleConf)
	if er != nil || len(indexDefs.IndexDefs) != 2 {
		t.Errorf("GetIndexDefs, err: %v", er)
	}

	indexDefn := indexDefs.IndexDefs["BEER"]
	if indexDefn == nil || indexDefn.UUID != "3c52d65b00000180" ||
		indexDefn.SourceName != "beer-sample" {
		t.Errorf("mismatched indexDef")
	}

	indexDefn = indexDefs.IndexDefs["TRAVEL"]
	if indexDefn == nil || indexDefn.UUID != "3c52d65b000001801" ||
		indexDefn.SourceName != "travel-sample" {
		t.Errorf("mismatched indexDef")
	}

	// reset the indexDefs, and veriy those are reflected in configs
	sampleIndexDef = []byte(`{"uuid":"3c52d65b00000180",
	"indexDefs":{"FTS":{"type":"fulltext-index","name":"FTS",
	"uuid":"3c52d65b00000180","sourceType":"couchbase",
	"sourceName":"beer-sample","sourceUUID":"77189afd8630ce46fd9a3b8a410fc4b9",
	"planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":
	{"docid_prefix_delim":"","docid_regexp":"","mode":"type_field",
	"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard",
	"default_datetime_parser":"dateTimeOptional","default_field":"_all",
	"default_mapping":{"dynamic":true,"enabled":true},
	"default_type":"_default","docvalues_dynamic":true,"index_dynamic":true,
	"store_dynamic":false,"type_field":"_type"},"store":
	{"indexType":"scorch","kvStoreName":""}},"sourceParams":{}}},
	"implVersion":"5.5.0"}`)

	conf = make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/indexDefs"] = sampleIndexDef
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	indexDefs, er = GetIndexDefs(sampleConf)
	if er != nil || len(indexDefs.IndexDefs) != 1 {
		t.Errorf("GetIndexDefs, err: %v", er)
	}

	indexDefn = indexDefs.IndexDefs["BEER"]
	if indexDefn != nil {
		t.Errorf("non existing indexDef")
	}

	indexDefn = indexDefs.IndexDefs["FTS"]
	if indexDefn == nil || indexDefn.UUID != "3c52d65b00000180" ||
		indexDefn.SourceName != "beer-sample" {
		t.Errorf("mismatched indexDef")
	}
	cleanConfig()
}

func TestRetrieveIndexDefs(t *testing.T) {
	sampleIndexDef := []byte(`{"uuid":"3c52d65b00000180",
	"indexDefs":{"FTS":{"type":"fulltext-index","name":"FTS",
	"uuid":"3c52d65b00000180","sourceType":"couchbase",
	"sourceName":"beer-sample","sourceUUID":"77189afd8630ce46fd9a3b8a410fc4b9",
	"planParams":{"maxPartitionsPerPIndex":171},"params":{"doc_config":
	{"docid_prefix_delim":"","docid_regexp":"","mode":"type_field",
	"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard",
	"default_datetime_parser":"dateTimeOptional","default_field":"_all",
	"default_mapping":{"dynamic":true,"enabled":true},
	"default_type":"_default","docvalues_dynamic":true,"index_dynamic":true,
	"store_dynamic":false,"type_field":"_type"},"store":
	{"indexType":"scorch","kvStoreName":""}},"sourceParams":{}}},
	"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	conf := make(map[string]interface{})
	conf["/fts/cbgt/cfg/indexDefs"] = sampleIndexDef

	sampleNodeDef := []byte(`{"uuid":"2c16140ab60bf05d",
	"nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	conf["/fts/cbgt/cfg/nodeDefs-known/5859d032c9cd9f1afb62cdf207c7d173"] = sampleNodeDef
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	ftsIndexer := &FTSIndexer{
		namespace: "test",
		keyspace:  "beer-sample",
	}

	ftsIndexer.SetCfg(sampleConf)

	indexMap, _, er := ftsIndexer.refreshConfigs()
	if er != nil {
		t.Fatal(er)
	}

	testIndex, exists := indexMap["3c52d65b00000180"]
	if !exists || testIndex == nil {
		t.Fatalf("index name 3c52d65b00000180 not found! %+v", indexMap)
	}

	if testIndex.KeyspaceId() != "beer-sample" ||
		testIndex.Name() != "FTS" ||
		testIndex.Id() != "3c52d65b00000180" {
		t.Fatalf("unexpected index attributes, "+
			"testIndex.KeyspaceId() actual: %v, "+
			"testIndex.Name() actual: %v, "+
			"testIndex.Id() actual: %v ", testIndex.KeyspaceId(),
			testIndex.Name(), testIndex.Id())
	}
	cleanConfig()
}

func TestGetNodeDefs(t *testing.T) {
	sampleNodeDef1 := []byte(`{"uuid":"2c16140ab60bf05d",
	"nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	conf := make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/nodeDefs-known/5859d032c9cd9f1afb62cdf207c7d173"] = sampleNodeDef1
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	var er error
	var nodeDefs *cbgt.NodeDefs
	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 1 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	nodeDefn := nodeDefs.NodeDefs["5859d032c9cd9f1afb62cdf207c7d173"]
	if nodeDefn == nil || nodeDefn.UUID != "5859d032c9cd9f1afb62cdf207c7d173" {
		t.Errorf("mismatched nodeDefs")
	}

	sampleNodeDef2 := []byte(`{"uuid":"1d62344e4476ef14","nodeDefs":{"d6ad6468930d9a57c4b7e90af0fb1bff":{"hostPort":"127.0.0.1:9203","uuid":"d6ad6468930d9a57c4b7e90af0fb1bff","implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)
	conf["/fts/cbgt/cfg/nodeDefs-known/d6ad6468930d9a57c4b7e90af0fb1bff"] = sampleNodeDef2
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 2 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}
	cleanConfig()
}

func TestRemoveNodeDefs(t *testing.T) {
	sampleNodeDef1 := []byte(`{"uuid":"2c16140ab60bf05d",
	"nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	conf := make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/nodeDefs-known/5859d032c9cd9f1afb62cdf207c7d173"] = sampleNodeDef1
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	var er error
	var nodeDefs *cbgt.NodeDefs
	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 1 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	nodeDefn := nodeDefs.NodeDefs["5859d032c9cd9f1afb62cdf207c7d173"]
	if nodeDefn == nil || nodeDefn.UUID != "5859d032c9cd9f1afb62cdf207c7d173" {
		t.Errorf("mismatched nodeDefs")
	}

	sampleNodeDef2 := []byte(`{"uuid":"1d62344e4476ef14","nodeDefs":{"d6ad6468930d9a57c4b7e90af0fb1bff":{"hostPort":"127.0.0.1:9203","uuid":"d6ad6468930d9a57c4b7e90af0fb1bff","implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)
	conf["/fts/cbgt/cfg/nodeDefs-known/d6ad6468930d9a57c4b7e90af0fb1bff"] = sampleNodeDef2
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	sampleNodeDef3 := []byte(`{"uuid":"70398b968d659d58","nodeDefs":{"aa32875f1b85c5d8b3f26a55ba6d251e":{"hostPort":"192.168.0.102:920","uuid":"aa32875f1b85c5d8b3f26a55ba6d251e","implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)
	conf["/fts/cbgt/cfg/nodeDefs-known/aa32875f1b85c5d8b3f26a55ba6d251e"] = sampleNodeDef3
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 3 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	// removing the node defs
	conf["/fts/cbgt/cfg/nodeDefs-known/d6ad6468930d9a57c4b7e90af0fb1bff"] = []byte(nil)
	conf["/fts/cbgt/cfg/nodeDefs-known/aa32875f1b85c5d8b3f26a55ba6d251e"] = []byte(nil)
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 1 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	// only the original node should remain
	nodeDefn = nodeDefs.NodeDefs["5859d032c9cd9f1afb62cdf207c7d173"]
	if nodeDefn == nil || nodeDefn.UUID != "5859d032c9cd9f1afb62cdf207c7d173" {
		t.Errorf("mismatched nodeDefs")
	}
	cleanConfig()
}
