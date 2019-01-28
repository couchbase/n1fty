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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/logging"
)

// Implementation of datastore.IndexConfig interface

const n1ftyTmpSpaceDir = "n1fty_tmpspace_dir"
const n1ftyTmpSpaceLimit = "n1fty_tmpspace_limit"
const n1ftySearchTimeoutMS = "n1fty_search_timeout_ms"

var defaultN1ftyBackfillLimit = int64(100)      // default tmp space limit
var defaultN1ftySearchTimeoutMS = int64(120000) // 2min

const n1ftyBackfillPrefix = "search-results"

var config n1ftyConfig

type n1ftyConfig struct {
	config atomic.Value
}

func GetN1ftyConfig() (datastore.IndexConfig, errors.Error) {
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
	for k, v := range conf {
		localconf[k] = v
	}

	logging.Infof("n1ftyConfig - Setting config %v", conf)
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

	if v, ok := conf[n1ftyTmpSpaceDir]; ok {
		if _, ok1 := v.(string); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				n1ftyTmpSpaceDir, v)
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[n1ftyTmpSpaceLimit]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				n1ftyTmpSpaceLimit, v)
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[n1ftySearchTimeoutMS]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config.. key: %v, val: %v",
				n1ftySearchTimeoutMS, v)
			return errors.NewError(err, err.Error())
		}
	}

	return nil
}

func (c *n1ftyConfig) processConfig(conf map[string]interface{}) {
	var olddir interface{}
	var newdir interface{}

	if conf != nil {
		newdir, _ = conf[n1ftyTmpSpaceDir]
	}

	prevconf := config.getConfig()

	if prevconf != nil {
		olddir, _ = prevconf[n1ftyTmpSpaceDir]
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

// best effor cleanup as tmpdir may change during restart
func cleanupTmpFiles(olddir string) {
	files, err := ioutil.ReadDir(olddir)
	if err != nil {
		return
	}

	searchTimeout := defaultN1ftySearchTimeoutMS
	conf := config.getConfig()
	if conf != nil {
		if val, ok := conf[n1ftySearchTimeoutMS]; ok {
			searchTimeout = val.(int64)
		}
	}

	for _, file := range files {
		fname := path.Join(olddir, file.Name())
		mtime := file.ModTime()
		since := (time.Since(mtime).Seconds() * 1000) * 2 // twice the long search
		if (strings.Contains(fname, "search-backfill") ||
			strings.Contains(fname, n1ftyBackfillPrefix)) &&
			int64(since) > searchTimeout {
			logging.Infof("n1fty: removing old file %v, last modified @ %v",
				fname, mtime)
			os.Remove(fname)
		}
	}
}

func (c *n1ftyConfig) getConfig() map[string]interface{} {
	conf := c.config.Load()
	if conf != nil {
		return conf.(map[string]interface{})
	} else {
		return nil
	}
}

func getDefaultTmpDir() string {
	file, err := ioutil.TempFile("" /*dir*/, n1ftyBackfillPrefix)
	if err != nil {
		return ""
	}

	default_temp_dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file

	return default_temp_dir
}
