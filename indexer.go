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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"

	"gopkg.in/couchbase/gocbcore.v7"
)

// FTSIndexer implements datastore.Indexer interface
type FTSIndexer struct {
	namespace string
	keyspace  string
	serverURL string

	agent *gocbcore.Agent

	// sync RWMutex protects following fields
	m sync.RWMutex

	lastRefreshTime time.Time

	indexIds   []string
	indexNames []string
	allIndexes []datastore.Index

	mapIndexesByID   map[string]datastore.Index
	mapIndexesByName map[string]datastore.Index

	cfg        Cfg
	srvWrapper *ftsSrvWrapper
	stats      *stats
	closeCh    chan struct{}
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

func NewFTSIndexer(serverIn, namespace, keyspace string) (datastore.Indexer,
	errors.Error) {
	logging.Infof("n1fty: server: %v, namespace: %v, keyspace: %v",
		serverIn, namespace, keyspace)

	server, _, bucketName :=
		cbgt.CouchbaseParseSourceName(serverIn, "default", keyspace)

	conf := &gocbcore.AgentConfig{
		UserString:           "n1fty",
		BucketName:           bucketName,
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		Auth:                 &Authenticator{},
	}

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, util.N1QLError(fmt.Errorf(
			"NewFTSIndexer, no servers provided"), "")
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
		namespace:       namespace,
		keyspace:        keyspace,
		serverURL:       svrs[0],
		agent:           agent,
		lastRefreshTime: time.Now(),
		cfg:             &config,
		stats:           &stats{},
		closeCh:         make(chan struct{}),
	}

	indexer.Refresh()

	go backfillMonitor(1*time.Second, indexer)
	// configurable interval later
	go logStats(60*time.Second, indexer)

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

// Close is an implementation of io.Closer interface
// It is recommended that query calls Close on the FTSIndexer
// object once its usage is over, for a graceful cleanup.
func (i *FTSIndexer) Close() error {
	close(i.closeCh)
	return nil
}

// SetCfg for better testing
func (i *FTSIndexer) SetCfg(cfg Cfg) {
	i.cfg = cfg
}

func (i *FTSIndexer) KeyspaceId() string {
	return i.keyspace
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

func (i *FTSIndexer) CreatePrimaryIndex(requestId, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, util.N1QLError(nil, "CreatePrimaryIndex not supported")
}

func (i *FTSIndexer) CreateIndex(requestId, name string,
	seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {
	return nil, util.N1QLError(nil, "CreateIndex not supported")
}

func (i *FTSIndexer) BuildIndexes(requestId string, name ...string) errors.Error {
	return util.N1QLError(nil, "BuildIndexes not supported")
}

func (i *FTSIndexer) Refresh() errors.Error {
	mapIndexesByID, nodeDefs, err := i.refreshConfigs()
	if err != nil {
		return util.N1QLError(err, "refresh failed")
	}

	err = i.initSrvWrapper(nodeDefs)
	if err != nil {
		return util.N1QLError(err, "initSrvWrapper failed")
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
	i.m.Unlock()

	return nil
}

func (i *FTSIndexer) MetadataVersion() uint64 {
	// FIXME
	return 0
}

func (i *FTSIndexer) SetLogLevel(level logging.Level) {
	logging.SetLevel(level)
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) refreshConfigs() (
	map[string]datastore.Index, *cbgt.NodeDefs, error) {
	var cfg Cfg
	cfg = &config
	if i.cfg != nil {
		cfg = i.cfg
	}

	var err error

	// first try to load configs from local config cache
	indexDefs, _ := GetIndexDefs(cfg)
	nodeDefs, _ := GetNodeDefs(cfg)

	// if not available in config, try fetching them from an fts node
	if indexDefs == nil || nodeDefs == nil {
		ftsEndpoints := i.agent.FtsEps()
		if len(ftsEndpoints) == 0 {
			logging.Infof("n1fty: no fts nodes available")
			return nil, nil, nil
		}
		now := time.Now().UnixNano()
		indexDefs, nodeDefs, err = i.retrieveIndexDefs(
			ftsEndpoints[now%int64(len(ftsEndpoints))])
		if err != nil {
			return nil, nil, err
		}
	}

	if indexDefs == nil {
		logging.Infof("n1fty: no index definitions available")
		return nil, nil, nil
	}

	if nodeDefs == nil {
		logging.Infof("n1fty: no node definitions available")
		return nil, nil, nil
	}

	imap, err := i.convertIndexDefs(indexDefs)
	return imap, nodeDefs, err
}

func (i *FTSIndexer) initSrvWrapper(nodeDefs *cbgt.NodeDefs) error {
	if nodeDefs == nil {
		return nil
	}

	hostMap, err := extractHostCertsMap(nodeDefs)
	if err != nil {
		return util.N1QLError(err, "indexer, extractHostCertsMap err")
	}

	i.srvWrapper, err = initRouter(hostMap, nil)
	if err != nil {
		return util.N1QLError(err, "indexer, initRouter err")
	}

	return nil
}

func (i *FTSIndexer) retrieveIndexDefs(node string) (
	*cbgt.IndexDefs, *cbgt.NodeDefs, error) {
	httpClient := i.agent.HttpClient()
	if httpClient == nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, client not available")
	}

	cbauthURL, err := cbgt.CBAuthURL(node + "/api/cfg")
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, err: %v", err)
	}

	resp, err := httpClient.Get(cbauthURL)
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, err: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, resp status code: %v",
			resp.StatusCode)
	}

	bodyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("n1fty: retrieveIndexDefs, resp body read err: %v", err)
	}

	var body struct {
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
		NodeDefs  *cbgt.NodeDefs  `json:"nodeDefsKnown"`
		Status    string          `json:"status"`
	}

	err = json.Unmarshal(bodyBuf, &body)
	if err != nil {
		return nil, nil, fmt.Errorf("retrieveIndexDefs, json err: %v", err)
	}

	if body.Status != "ok" {
		return nil, nil, fmt.Errorf("retrieveIndexDefs, status code: %s",
			body.Status)
	}

	return body.IndexDefs, body.NodeDefs, nil
}

// Convert FTS index definitions into a map of n1ql index id mapping to
// datastore.FTSIndex
func (i *FTSIndexer) convertIndexDefs(indexDefs *cbgt.IndexDefs) (
	map[string]datastore.Index, error) {
	rv := map[string]datastore.Index{}
	var err error
	for _, indexDef := range indexDefs.IndexDefs {
		if indexDef.SourceName != i.keyspace {
			// If the source name of the index definition doesn't
			// match the indexer's keyspace, do not include the
			// index.
			continue
		}
		searchFieldsMap, dynamicMapping, defaultAnalyzer :=
			util.SearchableFieldsForIndexDef(indexDef)
		if searchFieldsMap != nil || dynamicMapping {
			rv[indexDef.UUID], err = newFTSIndex(searchFieldsMap,
				dynamicMapping, defaultAnalyzer, indexDef, i)
			if err != nil {
				return nil, err
			}
		}
	}

	return rv, nil
}
