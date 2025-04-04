// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package n1fty

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	cbtls "github.com/couchbase/goutils/tls"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"

	"golang.org/x/net/http2"

	log "github.com/couchbase/clog"
)

const VERSION = 1

func loggerFunc(level, format string, args ...interface{}) string {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	prefix := ts + " [" + level + "] "
	if format != "" {
		return prefix + fmt.Sprintf(format, args...)
	}
	return prefix + fmt.Sprint(args...)
}

var initOnce sync.Once

func setLoggerCallback() {
	initOnce.Do(func() {
		log.SetLoggerCallback(loggerFunc)
	})
}

// FTSIndexer implements datastore.Indexer interface
type FTSIndexer struct {
	serverURL  string
	namespace  string
	keyspace   string
	bucket     string
	scope      string
	collection string

	collectionAware bool

	cfg     *ftsConfig
	stats   *stats
	closeCh chan struct{}
	init    sync.Once

	// sync RWMutex protects following fields
	m sync.RWMutex

	closed bool

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
	setLoggerCallback()

	logging.Infof("n1fty: NewFTSIndexer, server: %v, namespace: %v, keyspace: %v",
		serverIn, namespace, keyspace)

	return newFTSIndexer(serverIn, namespace, keyspace, "", "", keyspace)
}

// keyspace = collection
func NewFTSIndexer2(serverIn, namespace, bucket, scope, keyspace string) (
	datastore.Indexer, errors.Error) {
	setLoggerCallback()

	logging.Infof("n1fty: NewFTSIndexer2, server: %v, namespace: %v, bucket: %v,"+
		" scope: %v, keyspace: %v", serverIn, namespace, bucket, scope, keyspace)

	return newFTSIndexer(serverIn, namespace, bucket, scope, keyspace, keyspace)
}

// -----------------------------------------------------------------------------

func newFTSIndexer(serverIn, namespace, bucket, scope, collection, keyspace string) (
	datastore.Indexer, errors.Error) {
	server, _, bucketName :=
		cbgt.CouchbaseParseSourceName(serverIn, "default", bucket)

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, util.N1QLError(fmt.Errorf(
			"newFTSIndexer, no servers provided"), "")
	}

	indexer := &FTSIndexer{
		serverURL:       svrs[0],
		namespace:       namespace,
		keyspace:        keyspace,
		bucket:          bucketName,
		scope:           scope,
		collection:      collection,
		collectionAware: collection == keyspace,
		cfg:             srvConfig,
		stats:           &stats{},
		closeCh:         make(chan struct{}),
	}

	if len(scope) == 0 && len(collection) == 0 {
		// Initialize scope and collection to _default for when query
		// invokes NewFTSIndexer(..).
		//
		// This is needed because, query will use one indexer for both
		// the bucket situation and bucket._default._default, so
		// FTSIndexer1 will cater for indexes built against bucket and
		// those built against bucket._default._default.
		indexer.scope = "_default"
		indexer.collection = "_default"
	}

	// Register indexer as a metaKV config subscriber
	indexer.cfg.subscribe(
		indexer.namespace+"$"+indexer.bucket+"$"+indexer.scope+"$"+indexer.keyspace,
		indexer)

	indexer.cfg.bumpVersion()
	indexer.refresh(true)

	return indexer, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) SetConnectionSecurityConfig(
	conf *datastore.ConnectionSecurityConfig) {
	// Since we no longer have dedicated pools per indexer, we don't need to
	// update the pools on Indexer.SetConnectionSecurityConfig.
	// Instead, security config change will be handled by the
	// Datastore.SetConnectionSecurityConfig
}

// return true if refresh succeeded
func refreshSecurityConfig(conf *datastore.ConnectionSecurityConfig) bool {
	logging.Infof("n1fty: Receive security change notification")
	if conf == nil {
		logging.Warnf("n1fty: cannot refresh certificate, nil connection " +
			"security config")
		return false
	}

	if len(conf.CertFile) == 0 || len(conf.KeyFile) == 0 {
		logging.Warnf("n1fty: cannot refresh certificate, certificate location " +
			"is missing")
		return false
	}

	newSecurityConfig := &securityConfig{
		tlsPreference:              &conf.TLSConfig,
		clientAuthType:             &conf.TLSConfig.ClientAuthType,
		encryptionEnabled:          conf.ClusterEncryptionConfig.EncryptData,
		disableNonSSLPorts:         conf.ClusterEncryptionConfig.DisableNonSSLPorts,
		shouldClientsUseClientCert: conf.TLSConfig.ShouldClientsUseClientCert,
	}

	certificate, err := cbtls.LoadX509KeyPair(conf.CertFile, conf.KeyFile,
		conf.TLSConfig.PrivateKeyPassphrase)
	if err != nil {
		logging.Fatalf("n1fty: Failed to generate SSL certificate, err: %v", err)
		return false
	}
	newSecurityConfig.certificate = &certificate

	caFile := conf.CAFile
	if len(caFile) == 0 {
		caFile = conf.CertFile
	}

	newSecurityConfig.certInBytes, err = os.ReadFile(caFile)
	if err != nil {
		logging.Fatalf("n1fty: Failed to load certificate file, err: %v", err)
		return false
	}

	if newSecurityConfig.shouldClientsUseClientCert {
		clientCertificate, err := cbtls.LoadX509KeyPair(conf.InternalClientCertFile,
			conf.InternalClientKeyFile, conf.TLSConfig.ClientPrivateKeyPassphrase)
		if err != nil {
			logging.Fatalf("n1fty: Failed to generate client certificate, err: %v", err)
			return false
		}
		newSecurityConfig.clientCertificate = &clientCertificate
	}

	// used to get cluster level options like bleveMaxResultWindow
	updateHttpClient(newSecurityConfig.certInBytes)

	updateSecurityConfig(newSecurityConfig)

	logging.Infof("n1fty: Certificate refreshed successfully with "+
		"certFile %v, keyFile %v, caFile %v", conf.CertFile, conf.KeyFile,
		conf.CAFile)
	return true
}

// Indexer agnostic
func SetConnectionSecurityConfig(conf *datastore.ConnectionSecurityConfig) {
	if !refreshSecurityConfig(conf) {
		return
	}

	ftsClientInst.m.Lock()
	defer ftsClientInst.m.Unlock()

	err := ftsClientInst.updateConnPoolsLOCKED(securityConfigChange)
	if err != nil {
		logging.Errorf("n1fty: failed to update connection pools on "+
			"securityConfigChange, err: %v", err)
	}

	// Change in security settings such as encryption, disableNonSSLPorts
	// may require a topology change to update the connection pools.
	//
	// For example, if encryption is enabled, hosts list must be updated
	// to use SSL ports.

	err = ftsClientInst.updateConnPoolsLOCKED(topologyChange)
	if err != nil {
		logging.Errorf("n1fty: failed to update connection pools on "+
			"topologyChange, err: %v", err)
	}
}

// Close is an implementation of io.Closer interface
// It is recommended that query call Close on the FTSIndexer
// object once its usage is over, for a graceful cleanup.
func (i *FTSIndexer) Close() error {
	i.m.Lock()
	if i.closed {
		i.m.Unlock()
		return nil
	}
	i.closed = true
	i.m.Unlock()

	i.cfg.unSubscribe(i.namespace + "$" + i.bucket + "$" + i.scope + "$" + i.keyspace)
	mr.unregisterIndexer(i)
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

func (i *FTSIndexer) reset() {
	i.m.Lock()
	i.indexIds = nil
	i.indexNames = nil
	i.allIndexes = nil
	i.mapIndexesByID = nil
	i.mapIndexesByName = nil
	i.nodeDefs = nil
	i.client = nil
	i.m.Unlock()
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) refresh(force bool) errors.Error {
	if !force && (i.nodeDefs == nil || len(i.nodeDefs.NodeDefs) == 0) {
		// if no fts nodes available, then return
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

	// even with a forced refresh, if index/node definitions are nil,
	// then no need to spin supporting routines or
	// fetch the bleve max result window.
	if len(mapIndexesByID) == 0 ||
		nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		// reset any previous valid states
		i.reset()

		return nil
	}

	err = i.initClient(nodeDefs)
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

	// as it reaches here for the first time, all initialisations
	// looks good for the given FTSIndexer and hence spin off the
	// supporting go routines.
	i.init.Do(func() {
		mr.registerIndexer(i)

		// perform bleveMaxResultWindow initialisation only
		// once per FTSIndexer instance.
		bmrw, err := i.fetchBleveMaxResultWindow()
		if err == nil && uint64(bmrw) != util.GetBleveMaxResultWindow() {
			util.SetBleveMaxResultWindow(uint64(bmrw))
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
	indexDefs, err := GetIndexDefs(conf)
	if err != nil {
		logging.Infof("n1fty: GetIndexDefs, err: %v", err)
		return nil, nil, nil
	}

	nodeDefs, err := GetNodeDefs(conf)
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

func (i *FTSIndexer) getNodeDefs() *cbgt.NodeDefs {
	i.m.RLock()
	nodeDefs := i.nodeDefs
	i.m.RUnlock()
	return nodeDefs
}

func (i *FTSIndexer) initClient(nodeDefs *cbgt.NodeDefs) error {
	i.m.Lock()
	defer i.m.Unlock()
	if i.client != nil &&
		i.nodeDefsUnchangedLOCKED(nodeDefs) {
		return nil
	}

	if i.client == nil {
		i.client = ftsClientInst
	}

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
	nodeDefs := i.getNodeDefs()
	if nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		return 0, fmt.Errorf("no fts endpoints available")
	}

	var nodeDef *cbgt.NodeDef
	for _, nodeDef = range nodeDefs.NodeDefs {
		break
	}

	hostPortUrl := "http://" + nodeDef.HostPort
	if u, err := nodeDef.HttpsURL(); err == nil {
		hostPortUrl = u
	}

	cbauthURL, err := cbgt.CBAuthURL(hostPortUrl + "/api/manager")
	if err != nil {
		return 0, err
	}

	httpClient := obtainHttpClient()
	if httpClient == nil {
		return 0, fmt.Errorf("HttpClient unavailable")
	}

	resp, err := httpClient.Get(cbauthURL)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status code: %v", resp.StatusCode)
	}

	bodyBuf, err := io.ReadAll(resp.Body)
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
	if indexDefs == nil {
		return nil, nil
	}

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
				UUID:         indexDef.UUID,
				SourceName:   indexDef.SourceName,
				IMapping:     pip.IndexMapping,
				DocConfig:    pip.DocConfig,
				TypeMappings: pip.TypeMappings,
				Scope:        i.scope,
				Collection:   i.collection,
			})
		}
	}

	return rv, nil
}

// -----------------------------------------------------------------------------

var HttpClientTimeout = 60 * time.Second

var httpClientM sync.RWMutex
var httpClient *http.Client

func updateHttpClient(certInBytes []byte) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{},
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM(certInBytes); ok {
		transport.TLSClientConfig.RootCAs = rootCAs
		_ = http2.ConfigureTransport(transport)
	} else {
		transport.TLSClientConfig.InsecureSkipVerify = true
	}

	client := &http.Client{
		Timeout:   HttpClientTimeout,
		Transport: transport,
	}

	httpClientM.Lock()
	httpClient = client
	httpClientM.Unlock()
}

func obtainHttpClient() *http.Client {
	httpClientM.RLock()
	client := httpClient
	httpClientM.RUnlock()
	return client
}
