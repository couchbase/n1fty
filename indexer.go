// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package n1fty

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"

	"gopkg.in/couchbase/gocbcore.v7"
)

const VERSION = 1

var ConnectTimeoutMS = 60000      // 60s
var ServerConnectTimeoutMS = 7000 // 7s
var NmvRetryDelayMS = 100         // 0.1s

var BackfillMonitoringIntervalMS = 1000 // 1s
var StatsLoggingIntervalMS = 60000      // 60s

// FTSIndexer implements datastore.Indexer interface
type FTSIndexer struct {
	serverURL  string
	namespace  string
	keyspace   string
	bucket     string
	scope      string
	collection string

	collectionAware bool

	agent *gocbcore.Agent

	cfg     *ftsConfig
	stats   *stats
	closeCh chan struct{}
	init    sync.Once

	// sync RWMutex protects following fields
	m sync.RWMutex

	client   *ftsClient
	nodeDefs *cbgt.NodeDefs

	indexIds   []string
	indexNames []string
	allIndexes []datastore.Index

	mapIndexesByID   map[string]datastore.Index
	mapIndexesByName map[string]datastore.Index

	cfgVersion uint64
}

type stats struct {
	TotalSearch                int64
	TotalSearchDuration        int64
	TotalTTFBDuration          int64 // time to first response byte
	TotalThrottledFtsDuration  int64
	TotalThrottledN1QLDuration int64
	TotalBackFills             int64
	CurBackFillSize            int64
}

// -----------------------------------------------------------------------------

// keyspace = bucket
func NewFTSIndexer(serverIn, namespace, keyspace string) (datastore.Indexer,
	errors.Error) {
	logging.Infof("n1fty: NewFTSIndexer, server: %v, namespace: %v, keyspace: %v",
		serverIn, namespace, keyspace)

	return newFTSIndexer(serverIn, namespace, keyspace, "", "", keyspace)
}

// keyspace = collection
func NewFTSIndexer2(serverIn, namespace, bucket, scope, keyspace string) (
	datastore.Indexer, errors.Error) {
	logging.Infof("n1fty: NewFTSIndexer2, server: %v, namespace: %v, bucket: %v,"+
		" scope: %v, keyspace: %v", serverIn, namespace, bucket, scope, keyspace)

	return newFTSIndexer(serverIn, namespace, bucket, scope, keyspace, keyspace)
}

// -----------------------------------------------------------------------------

func newFTSIndexer(serverIn, namespace, bucket, scope, collection, keyspace string) (
	datastore.Indexer, errors.Error) {
	server, _, bucketName :=
		cbgt.CouchbaseParseSourceName(serverIn, "default", bucket)

	conf := &gocbcore.AgentConfig{
		UserString:           "n1fty",
		BucketName:           bucketName,
		ConnectTimeout:       time.Duration(ConnectTimeoutMS) * time.Millisecond,
		ServerConnectTimeout: time.Duration(ServerConnectTimeoutMS) * time.Millisecond,
		NmvRetryDelay:        time.Duration(NmvRetryDelayMS) * time.Millisecond,
		UseKvErrorMaps:       true,
		Auth:                 &Authenticator{},
	}

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, util.N1QLError(fmt.Errorf(
			"NewFTSIndexer2, no servers provided"), "")
	}

	err := conf.FromConnStr(svrs[0])
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	agent, err := gocbcore.CreateAgent(conf)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	indexer := &FTSIndexer{
		serverURL:       svrs[0],
		namespace:       namespace,
		keyspace:        keyspace,
		bucket:          bucket,
		scope:           scope,
		collection:      collection,
		collectionAware: collection == keyspace,
		agent:           agent,
		cfg:             srvConfig,
		stats:           &stats{},
		closeCh:         make(chan struct{}),
	}

	return indexer, nil
}

type Authenticator struct{}

func (a *Authenticator) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(
		endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocbcore.UserPassPair{{}}, err
	}

	return []gocbcore.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) SetConnectionSecurityConfig(
	conf *datastore.ConnectionSecurityConfig) {
	if conf == nil {
		return
	}

	newSecurityConfig := &securityConfig{
		tlsPreference:     &conf.TLSConfig,
		encryptionEnabled: conf.ClusterEncryptionConfig.EncryptData,
		disableNonSSLPort: conf.ClusterEncryptionConfig.DisableNonSSLPorts,
	}

	if len(conf.CertFile) != 0 && len(conf.KeyFile) != 0 {
		certificate, err := tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			logging.Fatalf("Failed to generate SSL certificate, err: %v", err)
		}
		newSecurityConfig.certificate = &certificate

		newSecurityConfig.certInBytes, err = ioutil.ReadFile(conf.CertFile)
		if err != nil {
			logging.Fatalf("Failed to load certificate file, err: %v", err)
		}
	}

	updateSecurityConfig(newSecurityConfig)

	// bump the cfg version as this needs a force refresh
	i.cfg.bumpVersion()

	i.Refresh()
}

// Close is an implementation of io.Closer interface
// It is recommended that query calls Close on the FTSIndexer
// object once its usage is over, for a graceful cleanup.
func (i *FTSIndexer) Close() error {
	i.cfg.unSubscribe(i.namespace + "$" + i.bucket + "$" + i.scope + "$" + i.keyspace)
	close(i.closeCh)
	return nil
}

// SetCfg for better testing
func (i *FTSIndexer) SetCfg(cfg *ftsConfig) {
	i.cfg = cfg
}

func (i *FTSIndexer) KeyspaceId() string {
	return i.keyspace
}

func (i *FTSIndexer) BucketId() string {
	return i.bucket
}

func (i *FTSIndexer) ScopeId() string {
	return i.scope
}

func (i *FTSIndexer) Name() datastore.IndexType {
	return datastore.FTS
}

func (i *FTSIndexer) IndexIds() ([]string, errors.Error) {
	if err := i.Refresh(); err != nil {
		return nil, err
	}

	i.m.RLock()
	indexIds := i.indexIds
	i.m.RUnlock()

	return indexIds, nil
}

func (i *FTSIndexer) IndexNames() ([]string, errors.Error) {
	if err := i.Refresh(); err != nil {
		return nil, err
	}

	i.m.RLock()
	indexNames := i.indexNames
	i.m.RUnlock()

	return indexNames, nil
}

func (i *FTSIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	// no refresh
	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesByID != nil {
		index, ok := i.mapIndexesByID[id]
		if ok {
			return index, nil
		}
	}

	return nil, util.N1QLError(nil,
		fmt.Sprintf("IndexById, fts index with id: %v not found", id))
}

func (i *FTSIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	// no refresh
	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesByName != nil {
		index, ok := i.mapIndexesByName[name]
		if ok {
			return index, nil
		}
	}

	return nil, util.N1QLError(nil,
		fmt.Sprintf("IndexByName, fts index with name: %v not found", name))
}

func (i *FTSIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return nil, nil
}

func (i *FTSIndexer) Indexes() ([]datastore.Index, errors.Error) {
	if err := i.Refresh(); err != nil {
		return nil, util.N1QLError(err, "")
	}

	i.m.RLock()
	allIndexes := i.allIndexes
	i.m.RUnlock()

	return allIndexes, nil
}

func (i *FTSIndexer) CreatePrimaryIndex(requestID, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, util.N1QLError(nil, "CreatePrimaryIndex not supported")
}

func (i *FTSIndexer) CreateIndex(requestID, name string,
	seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {
	return nil, util.N1QLError(nil, "CreateIndex not supported")
}

func (i *FTSIndexer) BuildIndexes(requestID string, name ...string) errors.Error {
	return util.N1QLError(nil, "BuildIndexes not supported")
}

func (i *FTSIndexer) Refresh() errors.Error {
	return i.refresh(false)
}

func (i *FTSIndexer) MetadataVersion() uint64 {
	return VERSION
}

func (i *FTSIndexer) SetLogLevel(level logging.Level) {
	logging.SetLevel(level)
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) refresh(configMutexAcquired bool) errors.Error {
	// if no fts nodes available, then return
	ftsEndpoints := i.agent.FtsEps()
	if len(ftsEndpoints) == 0 {
		return nil
	}

	// check whether the metakv configs have changed
	cfgVersion := i.cfg.getVersion()
	if cfgVersion == i.cfgVersion {
		return nil
	}

	mapIndexesByID, nodeDefs, err := i.refreshConfigs()
	if err != nil {
		return util.N1QLError(err, "refresh failed")
	}

	err = i.initClient(nodeDefs, true)
	if err != nil {
		if err == ErrFeatureUnavailable {
			return nil
		}
		return util.N1QLError(err, "initClient failed")
	}

	numIndexes := len(mapIndexesByID)
	indexIds := make([]string, 0, numIndexes)
	indexNames := make([]string, 0, numIndexes)
	allIndexes := make([]datastore.Index, 0, numIndexes)

	mapIndexesByName := map[string]datastore.Index{}

	for id, index := range mapIndexesByID {
		indexIds = append(indexIds, id)
		indexNames = append(indexNames, index.Name())
		allIndexes = append(allIndexes, index)
		mapIndexesByName[index.Name()] = index
	}

	i.m.Lock()
	i.indexIds = indexIds
	i.indexNames = indexNames
	i.allIndexes = allIndexes
	i.mapIndexesByID = mapIndexesByID
	i.mapIndexesByName = mapIndexesByName
	i.cfgVersion = cfgVersion
	i.m.Unlock()

	// even with a forced refresh, if both indexes and
	// node definitions are nil, then no need to spin
	// supporting routines or fetch the bleve max
	// result window.
	if mapIndexesByID == nil && nodeDefs == nil {
		// initialise the cfg before returning so that
		// the metakv changes will be subscribed
		i.cfg.initConfig()
		return nil
	}

	// as it reaches here for the first time, all initialisations
	// looks good for the given FTSIndexer and hence spin off the
	// supporting go routines.
	i.init.Do(func() {

		go backfillMonitor(
			time.Duration(BackfillMonitoringIntervalMS)*time.Millisecond, i)

		go logStats(time.Duration(StatsLoggingIntervalMS)*time.Millisecond, i)

		i.cfg.initConfig()

		if configMutexAcquired {
			i.cfg.subscribeLOCKED(i.namespace+"$"+i.bucket+"$"+i.scope+"$"+i.keyspace, i)
		} else {
			i.cfg.subscribe(i.namespace+"$"+i.bucket+"$"+i.scope+"$"+i.keyspace, i)
		}
		// perform bleveMaxResultWindow initialisation only
		// once per FTSIndexer instance.
		bmrw, err := i.fetchBleveMaxResultWindow()
		if err == nil && int64(bmrw) != util.GetBleveMaxResultWindow() {
			util.SetBleveMaxResultWindow(int64(bmrw))
		}
	})

	return nil
}

func (i *FTSIndexer) refreshConfigs() (
	map[string]datastore.Index, *cbgt.NodeDefs, error) {
	conf := srvConfig
	if i.cfg != nil {
		conf = i.cfg
	}

	// first try to load configs from meta kv cfg
	indexDefs, err := GetIndexDefs(conf.cfg)
	if err != nil {
		logging.Infof("n1fty: GetIndexDefs, err: %v", err)
		return nil, nil, nil
	}

	nodeDefs, err := GetNodeDefs(conf.cfg)
	if err != nil {
		logging.Infof("n1fty: GetNodeDefs, err: %v", err)
		return nil, nil, nil
	}

	if indexDefs == nil || nodeDefs == nil {
		return nil, nil, nil
	}

	imap, err := i.convertIndexDefs(indexDefs)
	return imap, nodeDefs, err
}

func (i *FTSIndexer) nodeDefsUnchangedLOCKED(newNodeDefs *cbgt.NodeDefs) bool {
	if i.nodeDefs == nil && newNodeDefs == nil {
		return true
	}

	if i.nodeDefs == nil || newNodeDefs == nil {
		return false
	}

	return i.nodeDefs.UUID == newNodeDefs.UUID
}

func (i *FTSIndexer) initClient(nodeDefs *cbgt.NodeDefs, force bool) error {
	i.m.Lock()
	defer i.m.Unlock()
	if !force && i.client != nil && i.nodeDefsUnchangedLOCKED(nodeDefs) {
		return nil
	}

	// setup new client
	client, err := setupFTSClient(nodeDefs)
	if err != nil {
		return err
	}

	if i.client != nil {
		i.client.Close()
	}

	i.client = client
	i.nodeDefs = nodeDefs

	return nil
}

func (i *FTSIndexer) getClient() *ftsClient {
	var client *ftsClient
	i.m.RLock()
	client = i.client
	i.m.RUnlock()
	return client
}

func (i *FTSIndexer) fetchBleveMaxResultWindow() (int, error) {
	ftsEndpoints := i.agent.FtsEps()
	if len(ftsEndpoints) == 0 {
		return 0, fmt.Errorf("no fts endpoints available")
	}

	now := time.Now().UnixNano()
	cbauthURL, err := cbgt.CBAuthURL(
		ftsEndpoints[now%int64(len(ftsEndpoints))] + "/api/manager")
	if err != nil {
		return 0, err
	}

	httpClient := i.agent.HttpClient()
	if httpClient == nil {
		return 0, fmt.Errorf("client not available")
	}

	resp, err := httpClient.Get(cbauthURL)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status code: %v", resp.StatusCode)
	}

	bodyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var expect map[string]interface{}
	err = json.Unmarshal(bodyBuf, &expect)
	if err != nil {
		return 0, err
	}

	if status, exists := expect["status"]; !exists || status.(string) != "ok" {
		return 0, err
	}

	if mgr, exists := expect["mgr"]; exists {
		mgrMap, _ := mgr.(map[string]interface{})
		options, _ := mgrMap["options"].(map[string]interface{})
		if bleveMaxResultWindow, exists := options["bleveMaxResultWindow"]; exists {
			return strconv.Atoi(bleveMaxResultWindow.(string))
		}
	}

	return 0, fmt.Errorf("value of bleveMaxResultWindow unknown")
}

// Convert FTS index definitions into a map of n1ql index id mapping to
// datastore.FTSIndex
func (i *FTSIndexer) convertIndexDefs(indexDefs *cbgt.IndexDefs) (
	map[string]datastore.Index, error) {
	rv := map[string]datastore.Index{}
	for _, indexDef := range indexDefs.IndexDefs {
		if !i.collectionAware {
			// TODO: Also check the keyspace's UUID (or, bucket's UUID)?
			if indexDef.SourceName != i.keyspace {
				// If the source name of the index definition doesn't
				// match the indexer's keyspace, do not include the index.
				continue
			}
		} else {
			// Retrieve collections supported from the index definition
			if indexDef.SourceName != i.bucket {
				// If the source name of the index definition doesn't
				// match the indexer's bucket, do not include the index.
				continue
			}

			scope, collections, err :=
				cbft.GetScopeCollectionsFromIndexDef(indexDef)
			if err != nil {
				continue
			}

			if scope != i.scope {
				// If the scope handled by the index definition doesn't
				// match the indexer's scope, do not include the index.
				continue
			}

			var collectionFound bool
			for _, coll := range collections {
				if i.keyspace == coll {
					collectionFound = true
					break
				}
			}

			if !collectionFound {
				// If the indexer's keyspace isn't one of the collections
				// that the index streams from, do not include the index.
				continue
			}
		}

		// If querying is disabled for the index, then skip it.
		if indexDef.PlanParams.NodePlanParams != nil {
			npp := indexDef.PlanParams.NodePlanParams[""][""]
			if npp != nil && !npp.CanRead {
				continue
			}
		}

		pip, err := util.ProcessIndexDef(indexDef, i.scope, i.collection)
		if err != nil {
			logging.Warnf("n1fty: error processing index definition for: %v, err: %v",
				indexDef.Name, err)
			continue
		}

		if len(pip.SearchFields) > 0 || len(pip.DynamicMappings) > 0 {
			rv[indexDef.UUID], err = newFTSIndex(i, indexDef, pip)
			if err != nil {
				logging.Warnf("n1fty: couldn't set up FTS index: %v for querying, err: %v",
					indexDef.Name, err)
				continue
			}

			// set this index mapping into the indexMappings cache
			util.SetIndexMapping(indexDef.Name, &util.MappingDetails{
				UUID:       indexDef.UUID,
				SourceName: indexDef.SourceName,
				IMapping:   pip.IndexMapping,
				DocConfig:  pip.DocConfig,
				Scope:      i.scope,
				Collection: i.collection,
			})
		}
	}

	return rv, nil
}

func (i *FTSIndexer) supportedByCluster() bool {
	i.m.RLock()
	defer i.m.RUnlock()

	if i.collectionAware {
		return (cbgt.IsFeatureSupportedByCluster(cbft.FeatureGRPC, i.nodeDefs) &&
			cbgt.IsFeatureSupportedByCluster(cbft.FeatureCollections, i.nodeDefs))
	}

	return cbgt.IsFeatureSupportedByCluster(cbft.FeatureGRPC, i.nodeDefs)
}
