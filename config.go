//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
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

// 5120 is the default tmp space limit shared between gsi/n1fty
var defaultBackfillLimit = int64(5120 / 2)
var defaultSearchTimeoutMS = int64(120000) // 2min
const backfillPrefix = "search-results"

// ftsConfig is the metakv config listener which helps the
// n1fty indexer to refresh it's config information like
// index/node definitions.
type ftsConfig struct {
	cfg     cbgt.Cfg
	eventCh chan cbgt.CfgEvent

	version uint64 // version for the metakv config changes

	m           sync.RWMutex
	subscribers map[string]datastore.Indexer

	m1        sync.RWMutex
	indexDefs *cbgt.IndexDefs
	nodeDefs  *cbgt.NodeDefs
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

var nodeDefsKnownKey = cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN)

func (c *ftsConfig) Listen() {
	for {
		select {
		case ev := <-c.eventCh:
			// first bump the version so that the subscribers can
			// verify the updated version with their cached one.
			atomic.AddUint64(&c.version, 1)

			if ev.Key == nodeDefsKnownKey {
				nodeDefs, _, err := cbgt.CfgGetNodeDefs(c.cfg, "known")
				if err != nil {
					logging.Infof("n1fty: ftsConfig CfgGetNodeDefs, err: %v", err)
					// reset the node defs so that the clients shall
					// fetch it directly from metakv.
					nodeDefs = nil
				}

				c.m1.Lock()
				c.nodeDefs = nodeDefs
				c.m1.Unlock()
			}

			if ev.Key == cbgt.INDEX_DEFS_KEY {
				indexDefs, _, err := cbgt.CfgGetIndexDefs(c.cfg)
				if err != nil {
					logging.Infof("n1fty: ftsConfig CfgGetIndexDefs, err: %v", err)
					// reset the index defs so that the clients shall
					// fetch it directly from metakv.
					indexDefs = nil
				}

				c.m1.Lock()
				c.indexDefs = indexDefs
				c.m1.Unlock()
			}

			// clear mappingsCache (used during Verify/Eval) to
			// remove any stale entries; the following refresh(..)
			// on every indexer will add the latest entries into
			// the cache.
			util.ClearMappingsCache()

			c.m.Lock()
			for _, i := range c.subscribers {
				if indexer, ok := i.(*FTSIndexer); ok {
					indexer.refresh(true)
				}
			}
			c.m.Unlock()
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
	c.subscribeLOCKED(key, i)
	c.m.Unlock()
}

func (c *ftsConfig) subscribeLOCKED(key string, i datastore.Indexer) {
	c.subscribers[key] = i
}

func (c *ftsConfig) unSubscribe(key string) {
	c.m.Lock()
	delete(c.subscribers, key)
	c.m.Unlock()
}

func (c *ftsConfig) getVersion() uint64 {
	return atomic.LoadUint64(&c.version)
}

func (c *ftsConfig) bumpVersion() {
	atomic.AddUint64(&c.version, 1)
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
	for k, v := range conf {
		newConf[k] = v
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
func GetIndexDefs(srvConfig *ftsConfig) (*cbgt.IndexDefs, error) {
	srvConfig.m1.RLock()
	rv := srvConfig.indexDefs
	srvConfig.m1.RUnlock()
	if rv != nil {
		return rv, nil
	}

	// fetch it from metakv only when the local cache is empty.
	indexDefs, _, err := cbgt.CfgGetIndexDefs(srvConfig.cfg)
	if err != nil {
		return nil, fmt.Errorf("indexDefs err: %v", err)
	}

	srvConfig.m1.Lock()
	srvConfig.indexDefs = indexDefs
	srvConfig.m1.Unlock()

	return indexDefs, nil
}

// GetNodeDefs gets the latest nodeDefs from configs
func GetNodeDefs(srvConfig *ftsConfig) (*cbgt.NodeDefs, error) {
	srvConfig.m1.RLock()
	rv := srvConfig.nodeDefs
	srvConfig.m1.RUnlock()
	if rv != nil {
		return rv, nil
	}

	// fetch it from metakv only when the local cache is empty.
	nodeDefs, _, err := cbgt.CfgGetNodeDefs(srvConfig.cfg, "known")
	if err != nil {
		return nil, fmt.Errorf("nodeDefs err: %v", err)
	}

	srvConfig.m1.Lock()
	srvConfig.nodeDefs = nodeDefs
	srvConfig.m1.Unlock()

	return nodeDefs, nil
}

// -----------------------------------------------------------------------------

// Atomic updates to variable to determine if system is
// in serverless mode or not - can take values 0 or 1 only.
var serverlessMode uint32

// SetDeploymentModel lets n1fty know the mode of operation.
func SetDeploymentModel(serverless bool) {
	if serverless {
		atomic.StoreUint32(&serverlessMode, 1)
	} else {
		atomic.StoreUint32(&serverlessMode, 0)
	}
}

func IsServerlessMode() bool {
	return atomic.LoadUint32(&serverlessMode) == 1
}
