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
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/logging"
)

const backfillSpaceDir = "query_tmpspace_dir"
const backfillSpaceLimit = "query_tmpspace_limit"
const searchTimeoutMS = "searchTimeoutMS"

var defaultBackfillLimit = int64(200)      // default tmp space limit
var defaultSearchTimeoutMS = int64(120000) // 2min

const backfillPrefix = "search-results"

type Cfg interface {
	datastore.IndexConfig
	GetConfig() map[string]interface{}
}

var config n1ftyConfig

// n1ftyConfig implementation of datastore.IndexConfig interface
type n1ftyConfig struct {
	config atomic.Value
}

func GetConfig() (datastore.IndexConfig, errors.Error) {
	return &config, nil
}

func setConfig(nf *n1ftyConfig, conf map[string]interface{}) errors.Error {
	err := nf.validateConfig(conf)
	if err != nil {
		return err
	}

	nf.processConfig(conf)

	// make local copy so caller doesn't accidentally modify
	newConf := nf.GetConfig()
	if newConf == nil {
		newConf = make(map[string]interface{})
	}
	var b []byte
	var ok bool
	for k, v := range conf {
		if b, ok = v.([]byte); !ok {
			continue
		}

		if strings.Contains(k, "nodeDefs-known") {
			// may be a node deletion
			if len(b) == 0 {
				delete(newConf, k)
				continue
			}

			var nodeDefs cbgt.NodeDefs
			err := json.Unmarshal(b, &nodeDefs)
			if err != nil {
				continue
			}
			newConf[k] = &nodeDefs
			continue
		}

		li := strings.LastIndex(k, "/")
		leaf := k[li+1:]
		if leaf == "indexDefs" {
			var indexDefs cbgt.IndexDefs
			err := json.Unmarshal(b, &indexDefs)
			if err != nil {
				continue
			}
			newConf[leaf] = &indexDefs
		}
	}

	nf.config.Store(newConf)
	return nil
}

func (c *n1ftyConfig) SetConfig(conf map[string]interface{}) errors.Error {
	return setConfig(c, conf)
}

// SetParam should not be called concurrently with SetConfig
func (c *n1ftyConfig) SetParam(name string, val interface{}) errors.Error {
	conf := c.config.Load().(map[string]interface{})

	if conf != nil {
		tempconf := make(map[string]interface{})
		tempconf[name] = val
		err := c.validateConfig(tempconf)
		if err != nil {
			return err
		}
		c.processConfig(tempconf)
		logging.Infof("n1ftyConfig - Setting param %v %v", name, val)
		conf[name] = val
	} else {
		conf = make(map[string]interface{})
		conf[name] = val
		return c.SetConfig(conf)
	}
	return nil
}

func (c *n1ftyConfig) validateConfig(conf map[string]interface{}) errors.Error {
	if conf == nil {
		return nil
	}

	if v, ok := conf[backfillSpaceDir]; ok {
		if _, ok1 := v.(string); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				backfillSpaceDir, v)
			return util.N1QLError(err, err.Error())
		}
	}

	if v, ok := conf[backfillSpaceLimit]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				backfillSpaceLimit, v)
			return util.N1QLError(err, err.Error())
		}
	}

	if v, ok := conf[searchTimeoutMS]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				searchTimeoutMS, v)
			return util.N1QLError(err, err.Error())
		}
	}

	return nil
}

func (c *n1ftyConfig) processConfig(conf map[string]interface{}) {
	var olddir interface{}
	var newdir interface{}

	if conf != nil {
		newdir, _ = conf[backfillSpaceDir]
	}

	prevconf := config.GetConfig()

	if prevconf != nil {
		olddir, _ = prevconf[backfillSpaceDir]
	}

	if olddir == nil {
		olddir = getDefaultTmpDir()
	}

	// cleanup any stale files
	if olddir != newdir {
		cleanupTmpFiles(olddir.(string))
		if newdir != nil {
			cleanupTmpFiles(newdir.(string))
		}
	}
}

// best effort cleanup as tmpdir may change during restart
func cleanupTmpFiles(olddir string) {
	files, err := ioutil.ReadDir(olddir)
	if err != nil {
		return
	}

	searchTimeout := defaultSearchTimeoutMS
	conf := config.GetConfig()
	if conf != nil {
		if val, ok := conf[searchTimeoutMS]; ok {
			searchTimeout = val.(int64)
		}
	}

	for _, file := range files {
		fname := path.Join(olddir, file.Name())
		mtime := file.ModTime()
		since := (time.Since(mtime).Seconds() * 1000) * 2 // twice the long search
		if (strings.Contains(fname, backfillPrefix)) &&
			int64(since) > searchTimeout {
			logging.Infof("n1fty: removing old file %v, last modified @ %v",
				fname, mtime)
			os.Remove(fname)
		}
	}
}

func (c *n1ftyConfig) GetConfig() map[string]interface{} {
	conf := c.config.Load()
	if conf != nil {
		return conf.(map[string]interface{})
	}
	return nil
}

func getDefaultTmpDir() string {
	file, err := ioutil.TempFile("" /*dir*/, backfillPrefix)
	if err != nil {
		return ""
	}

	default_temp_dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file

	return default_temp_dir
}

// GetIndexDefs gets the latest indexDefs from configs
func GetIndexDefs(cfg Cfg) (*cbgt.IndexDefs, error) {
	conf := cfg.GetConfig()
	if conf != nil {
		if v, ok := conf["indexDefs"]; ok {
			if defs, ok := v.(*cbgt.IndexDefs); ok {
				return defs, nil
			}
			return nil, fmt.Errorf("no indexDefs found")
		}
	}
	return nil, fmt.Errorf("no config found")
}

// GetNodeDefs gets the latest nodeDefs from configs
func GetNodeDefs(cfg Cfg) (*cbgt.NodeDefs, error) {
	conf := cfg.GetConfig()
	rv := cbgt.NodeDefs{
		NodeDefs: make(map[string]*cbgt.NodeDef, 1),
	}

	if conf != nil {
		uuids := []string{}
		for k, v := range conf {
			if strings.Contains(k, "nodeDefs-known") {
				if defs, ok := v.(*cbgt.NodeDefs); ok {
					for k1, v1 := range defs.NodeDefs {
						rv.NodeDefs[k1] = v1
					}
					// use the lowest version among nodeDefs
					if rv.ImplVersion == "" ||
						!cbgt.VersionGTE(defs.ImplVersion, defs.ImplVersion) {
						rv.ImplVersion = defs.ImplVersion
					}
					uuids = append(uuids, defs.UUID)
				}
			}
		}
		rv.UUID = checkSumUUIDs(uuids)
		return &rv, nil
	}
	return nil, fmt.Errorf("no config found")
}

func checkSumUUIDs(uuids []string) string {
	sort.Strings(uuids)
	d, _ := json.Marshal(uuids)
	return fmt.Sprint(crc32.ChecksumIEEE(d))
}
