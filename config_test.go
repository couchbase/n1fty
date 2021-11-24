//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/errors"
	"testing"
)

var tconfig *ftsConfig

func init() {
	tconfig = &ftsConfig{
		cfg: cbgt.NewCfgMem(),
	}
}
func GetTestConfig() (cbgt.Cfg, errors.Error) {
	return tconfig.cfg, nil
}

func cleanConfig() {
	sampleConf, _ := GetTestConfig()
	sampleConf.Del("indexDefs", 0)
	sampleConf.Del("nodeDefs-known", 0)
	sampleConf.Del("nodeDefs-known", 0)
	sampleConf.Del("nodeDefs-known", 0)
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

	_, err1 := sampleConf.Set("indexDefs", sampleIndexDef, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
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

	_, err1 := sampleConf.Set("indexDefs", sampleIndexDef, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
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

	_, err1 = sampleConf.Set("indexDefs", sampleIndexDef, cbgt.CFG_CAS_FORCE)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
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

	_, err1 := sampleConf.Set("indexDefs", sampleIndexDef, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
	}

	sampleNodeDef := []byte(`{"uuid":"2c16140ab60bf05d",
	"nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	_, err1 = sampleConf.Set("nodeDefs-known", sampleNodeDef, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	ftsIndexer := &FTSIndexer{
		namespace: "test",
		keyspace:  "beer-sample",
	}

	ftsIndexer.SetCfg(tconfig)

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
	sampleNodeDef1 := []byte(`{"uuid":"2c16140ab60bf05d","nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173","implVersion":"5.5.0",
	"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
	"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"},
	"d6ad6468930d9a57c4b7e90af0fb1bff":{"hostPort":"127.0.0.1:9203","uuid":"d6ad6468930d9a57c4b7e90af0fb1bff",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	_, err1 := sampleConf.Set("nodeDefs-known", sampleNodeDef1, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
	}

	var er error
	var nodeDefs *cbgt.NodeDefs
	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 2 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	nodeDefn := nodeDefs.NodeDefs["5859d032c9cd9f1afb62cdf207c7d173"]
	if nodeDefn == nil || nodeDefn.UUID != "5859d032c9cd9f1afb62cdf207c7d173" {
		t.Errorf("mismatched nodeDefs")
	}

	nodeDefn = nodeDefs.NodeDefs["d6ad6468930d9a57c4b7e90af0fb1bff"]
	if nodeDefn == nil || nodeDefn.UUID != "d6ad6468930d9a57c4b7e90af0fb1bff" {
		t.Errorf("mismatched nodeDefs")
	}

	cleanConfig()
}

func TestRemoveNodeDefs(t *testing.T) {
	sampleNodeDef1 := []byte(`{"uuid":"2c16140ab60bf05d",
	"nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":{"hostPort":"172.16.1.90:9200",
	"uuid":"5859d032c9cd9f1afb62cdf207c7d173","implVersion":"5.5.0",
	"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
	"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"},
	"d6ad6468930d9a57c4b7e90af0fb1bff":{"hostPort":"127.0.0.1:9203",
	"uuid":"d6ad6468930d9a57c4b7e90af0fb1bff","implVersion":"5.5.0",
	"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
	"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"},
	"aa32875f1b85c5d8b3f26a55ba6d251e":{"hostPort":"192.168.0.102:920",
	"uuid":"aa32875f1b85c5d8b3f26a55ba6d251e","implVersion":"5.5.0",
	"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
	"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	sampleConf, err := GetTestConfig()
	if err != nil {
		t.Errorf("GetTestConfig, err: %v", err)
	}

	_, err1 := sampleConf.Set("nodeDefs-known", sampleNodeDef1, 0)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
	}

	var er error
	var nodeDefs *cbgt.NodeDefs
	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 3 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	nodeDefn := nodeDefs.NodeDefs["aa32875f1b85c5d8b3f26a55ba6d251e"]
	if nodeDefn == nil || nodeDefn.UUID != "aa32875f1b85c5d8b3f26a55ba6d251e" {
		t.Errorf("mismatched nodeDefs")
	}

	sampleNodeDef2 := []byte(`{"uuid":"2c16140ab60bf05d","nodeDefs":{"5859d032c9cd9f1afb62cdf207c7d173":
	{"hostPort":"172.16.1.90:9200","uuid":"5859d032c9cd9f1afb62cdf207c7d173","implVersion":"5.5.0",
	"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
	"extras":"{\"bindGRPC\":\"172.16.1.90:9202\",\"bindHTTPS\":\":9201\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"172.16.1.90:9000\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"},
	"d6ad6468930d9a57c4b7e90af0fb1bff":{"hostPort":"127.0.0.1:9203","uuid":"d6ad6468930d9a57c4b7e90af0fb1bff",
	"implVersion":"5.5.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
	"container":"","weight":1,"extras":"{\"bindGRPC\":\"127.0.0.1:9205\",\"bindHTTPS\":\":9204\",\"features\":\"leanPlan,indexType:scorch,indexType:upside_down\",\"nsHostPort\":\"127.0.0.1:9001\",\"tlsCertPEM\":\"\",\"version-cbft.app\":\"v0.6.0\",\"version-cbft.lib\":\"v0.5.5\"}"}},"implVersion":"5.5.0"}`)

	_, err1 = sampleConf.Set("nodeDefs-known", sampleNodeDef2, cbgt.CFG_CAS_FORCE)
	if err1 != nil {
		t.Errorf("SetConfig, err: %v", err1)
	}

	nodeDefs, er = GetNodeDefs(sampleConf)
	if er != nil || len(nodeDefs.NodeDefs) != 2 {
		t.Errorf("GetNodeDefs, err: %v", er)
	}

	// only the original node should remain
	nodeDefn = nodeDefs.NodeDefs["5859d032c9cd9f1afb62cdf207c7d173"]
	if nodeDefn == nil || nodeDefn.UUID != "5859d032c9cd9f1afb62cdf207c7d173" {
		t.Errorf("mismatched nodeDefs")
	}

	nodeDefn = nodeDefs.NodeDefs["aa32875f1b85c5d8b3f26a55ba6d251e"]
	if nodeDefn != nil {
		t.Errorf("mismatched nodeDefs")
	}
	cleanConfig()
}
