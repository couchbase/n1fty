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
	"sync"
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

const metakvMetaDir = "/fts/cbgt/cfg/"

var defaultBackfillLimit = int64(200)      // default tmp space limit
var defaultSearchTimeoutMS = int64(120000) // 2min
const backfillPrefix = "search-results"

// ftsConfig is the metakv config listener which helps the
// n1fty indexer to refresh it's config information like
// index/node definitions.
type ftsConfig struct {
	cfg     cbgt.Cfg
	eventCh chan cbgt.CfgEvent

	m           sync.RWMutex
	version     uint64 // version for the metakv config changes
	subscribers map[string]datastore.Indexer
}

var once sync.Once

var srvConfig *ftsConfig

func init() {
	var err error
	srvConfig = &ftsConfig{
		eventCh:     make(chan cbgt.CfgEvent),
		subscribers: make(map[string]datastore.Indexer),
		version:     1,
	}
	cbgt.CfgMetaKvPrefix = "/fts/cbgt/cfg/"
	srvConfig.cfg, err = cbgt.NewCfgMetaKv("", make(map[string]string))
	if err != nil {
		logging.Infof("n1fty: ftsConfig err: %v", err)
	}

	go srvConfig.Listen()
}

func (c *ftsConfig) Listen() {
	for {
		select {
		case <-c.eventCh:
			// first bump the version so that the subscribers can
			// verify the updated version with their cached one.
			c.m.Lock()
			c.version++
			c.m.Unlock()

			c.m.RLock()
			for _, i := range c.subscribers {
				i.Refresh()
			}
			c.m.RUnlock()
		}
	}
}

func (c *ftsConfig) initConfig() {
	once.Do(func() {
		c.cfg.Subscribe(cbgt.INDEX_DEFS_KEY, c.eventCh)
		c.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN), c.eventCh)
	})
}

func (c *ftsConfig) subscribe(key string, i datastore.Indexer) {
	c.m.Lock()
	c.subscribers[key] = i
	c.m.Unlock()
}

func (c *ftsConfig) unSubscribe(key string) {
	c.m.Lock()
	delete(c.subscribers, key)
	c.m.Unlock()
}

func (c *ftsConfig) getVersion() uint64 {
	c.m.RLock()
	rv := c.version
	c.m.RUnlock()
	return rv
}

type Cfg interface {
	datastore.IndexConfig
	GetConfig() map[string]interface{}
}

// clientConfig is used by the query to pass on the configs values
// related to the backfill.
var clientConfig n1ftyConfig

// n1ftyConfig implementation of datastore.IndexConfig interface
type n1ftyConfig struct {
	config atomic.Value
}

func GetConfig() (datastore.IndexConfig, errors.Error) {
	return &clientConfig, nil
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

	prevconf := clientConfig.GetConfig()

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
	conf := clientConfig.GetConfig()
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
	file, err := ioutil.TempFile("", backfillPrefix)
	if err != nil {
		return ""
	}
	defaultDir := path.Dir(file.Name())
	os.Remove(file.Name())
	return defaultDir
}

// GetIndexDefs gets the latest indexDefs from configs
func GetIndexDefs(cfg cbgt.Cfg) (*cbgt.IndexDefs, error) {
	indexDefs, _, err := cbgt.CfgGetIndexDefs(cfg)
	if err != nil {
		return nil, fmt.Errorf("indexDefs err: %v", err)
	}
	return indexDefs, nil
}

// GetNodeDefs gets the latest nodeDefs from configs
func GetNodeDefs(cfg cbgt.Cfg) (*cbgt.NodeDefs, error) {
	nodeDefs, _, err := cbgt.CfgGetNodeDefs(cfg, "known")
	if err != nil {
		return nil, fmt.Errorf("nodeDefs err: %v", err)
	}
	return nodeDefs, nil
}
