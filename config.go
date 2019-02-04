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
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/logging"
)

// Implementation of datastore.IndexConfig interface

const tmpSpaceDir = "n1fty_tmpspace_dir"
const tmpSpaceLimit = "n1fty_tmpspace_limit"
const searchTimeoutMS = "n1fty_search_timeout_ms"

var defaultBackfillLimit = int64(100)      // default tmp space limit
var defaultSearchTimeoutMS = int64(120000) // 2min

const backfillPrefix = "search-results"

type Cfg interface {
	datastore.IndexConfig
	GetConfig() map[string]interface{}
}

var config n1ftyConfig

type n1ftyConfig struct {
	config atomic.Value
}

func GetConfig() (datastore.IndexConfig, errors.Error) {
	return &config, nil
}

func (c *n1ftyConfig) SetConfig(conf map[string]interface{}) errors.Error {
	err := c.validateConfig(conf)
	if err != nil {
		return err
	}

	c.processConfig(conf)

	// make local copy so caller doesn't accidentally modify
	localconf := make(map[string]interface{})
	var b []byte
	var ok bool
	for k, v := range conf {
		if b, ok = v.([]byte); !ok {
			continue
		}
		li := strings.LastIndex(k, "/")
		k = k[li+1:]
		// currently listen only to indexDef changes
		switch k {
		case "indexDefs":
			var indexDefs cbgt.IndexDefs
			err := json.Unmarshal(b, &indexDefs)
			if err != nil {
				continue
			}
			localconf[k] = &indexDefs
		}
	}

	c.config.Store(localconf)
	return nil
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

	if v, ok := conf[tmpSpaceDir]; ok {
		if _, ok1 := v.(string); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				tmpSpaceDir, v)
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[tmpSpaceLimit]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				tmpSpaceLimit, v)
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[searchTimeoutMS]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				searchTimeoutMS, v)
			return errors.NewError(err, err.Error())
		}
	}

	return nil
}

func (c *n1ftyConfig) processConfig(conf map[string]interface{}) {
	var olddir interface{}
	var newdir interface{}

	if conf != nil {
		newdir, _ = conf[tmpSpaceDir]
	}

	prevconf := config.GetConfig()

	if prevconf != nil {
		olddir, _ = prevconf[tmpSpaceDir]
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
