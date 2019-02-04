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
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/query/errors"
)

var tconfig testConfig

type testConfig struct {
	config atomic.Value
}

func GetTestConfig() (Cfg, errors.Error) {
	return &tconfig, nil
}

func (c *testConfig) SetConfig(conf map[string]interface{}) errors.Error {
	localconf := make(map[string]interface{})
	var b []byte
	var ok bool
	for k, v := range conf {
		if b, ok = v.([]byte); !ok {
			continue
		}
		li := strings.LastIndex(k, "/")
		k = k[li+1:]

		switch k {
		case "indexDefs":
			var indexDefs cbgt.IndexDefs
			err := json.Unmarshal(b, &indexDefs)
			if err != nil {
				log.Printf("json err: %v", err)
				continue
			}
			localconf[k] = &indexDefs
		}
	}
	c.config.Store(localconf)
	return nil
}

func (c *testConfig) SetParam(name string, val interface{}) errors.Error {
	return nil
}

func (c *testConfig) GetConfig() map[string]interface{} {
	conf := c.config.Load()
	if conf != nil {
		return conf.(map[string]interface{})
	}
	return nil
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

	conf := make(map[string]interface{}, 1)
	conf["/fts/cbgt/cfg/indexDefs"] = sampleIndexDef
	err = sampleConf.SetConfig(conf)
	if err != nil {
		t.Errorf("SetConfig, err: %v", err)
	}

	ftsIndexer := &FTSIndexer{
		namespace: "test",
		keyspace:  "beer-sample",
	}

	ftsIndexer.SetCfg(sampleConf)

	indexMap, er := ftsIndexer.refreshIndexes()
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
}
