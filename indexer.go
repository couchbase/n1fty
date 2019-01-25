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

	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"

	"gopkg.in/couchbase/gocbcore.v7"
)

// Implements datastore.Indexer interface
type FTSIndexer struct {
	namespace string
	keyspace  string

	agent *gocbcore.Agent

	// sync RWMutex protects following fields
	m sync.RWMutex

	lastRefreshTime time.Time

	indexIds   []string
	indexNames []string
	allIndexes []datastore.Index

	mapIndexesById   map[string]datastore.Index
	mapIndexesByName map[string]datastore.Index

	// FIXME: Stats, config
}

// -----------------------------------------------------------------------------

func NewFTSIndexer(server, namespace, keyspace string) (datastore.Indexer,
	errors.Error) {
	logging.Infof("n1fty: server: %v, namespace: %v, keyspace: %v",
		server, namespace, keyspace)

	config := &gocbcore.AgentConfig{
		UserString:           "n1fty",
		BucketName:           keyspace,
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		Auth:                 &Authenticator{},
	}

	svrs := strings.Split(server, ";")
	if len(svrs) <= 0 {
		return nil, errors.NewError(fmt.Errorf(
			"NewFTSIndexer: no servers provided"), "")
	}

	err := config.FromConnStr(svrs[0])
	if err != nil {
		return nil, errors.NewError(err, "")
	}

	agent, err := gocbcore.CreateAgent(config)
	if err != nil {
		return nil, errors.NewError(err, "")
	}

	indexer := &FTSIndexer{
		namespace:       namespace,
		keyspace:        keyspace,
		agent:           agent,
		lastRefreshTime: time.Now(),
	}

	err = indexer.Refresh()
	if err != nil {
		return nil, errors.NewError(err, "n1fty: Refresh err")
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

func (i *FTSIndexer) KeyspaceId() string {
	return i.keyspace
}

func (i *FTSIndexer) Name() datastore.IndexType {
	return datastore.FTS
}

func (i *FTSIndexer) IndexIds() ([]string, errors.Error) {
	if err := i.refresh(); err != nil {
		return nil, errors.NewError(err, "")
	}

	i.m.RLock()
	indexIds := i.indexIds
	i.m.RUnlock()

	return indexIds, nil
}

func (i *FTSIndexer) IndexNames() ([]string, errors.Error) {
	if err := i.refresh(); err != nil {
		return nil, errors.NewError(err, "")
	}

	i.m.RLock()
	indexNames := i.indexNames
	i.m.RUnlock()

	return indexNames, nil
}

func (i *FTSIndexer) IndexById(id string) (datastore.Index, errors.Error) {
	if err := i.refresh(); err != nil {
		return nil, errors.NewError(err, "")
	}

	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesById != nil {
		index, ok := i.mapIndexesById[id]
		if ok {
			return index, nil
		}
	}

	return nil, errors.NewError(nil,
		fmt.Sprintf("IndexById: fts index with id: %v not found", id))

}

func (i *FTSIndexer) IndexByName(name string) (datastore.Index, errors.Error) {
	if err := i.refresh(); err != nil {
		return nil, errors.NewError(err, "")
	}

	i.m.RLock()
	defer i.m.RUnlock()
	if i.mapIndexesByName != nil {
		index, ok := i.mapIndexesByName[name]
		if ok {
			return index, nil
		}
	}

	return nil, errors.NewError(nil,
		fmt.Sprintf("IndexByName: fts index with name: %v not found", name))
}

func (i *FTSIndexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "not supported")
}

func (i *FTSIndexer) Indexes() ([]datastore.Index, errors.Error) {
	if err := i.refresh(); err != nil {
		return nil, errors.NewError(err, "")
	}

	i.m.RLock()
	allIndexes := i.allIndexes
	i.m.RUnlock()

	return allIndexes, nil
}

func (i *FTSIndexer) CreatePrimaryIndex(requestId, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "not supported")
}

func (i *FTSIndexer) CreateIndex(requestId, name string,
	seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {
	return nil, errors.NewError(nil, "not supported")
}

func (i *FTSIndexer) BuildIndexes(requestId string, name ...string) errors.Error {
	return errors.NewError(nil, "not supported")
}

func (i *FTSIndexer) Refresh() errors.Error {
	return i.refresh()
}

func (i *FTSIndexer) MetadataVersion() uint64 {
	//FIXME
	return 0
}

func (i *FTSIndexer) SetLogLevel(level logging.Level) {
	logging.SetLevel(level)
}

// -----------------------------------------------------------------------------

func (i *FTSIndexer) refresh() errors.Error {
	mapIndexesById, err := i.getLatestIndexSet()
	if err != nil {
		return errors.NewError(err, "refresh failed")
	}

	numIndexes := len(mapIndexesById)
	indexIds := make([]string, 0, numIndexes)
	indexNames := make([]string, 0, numIndexes)
	allIndexes := make([]datastore.Index, 0, numIndexes)

	mapIndexesByName := map[string]datastore.Index{}

	for id, index := range mapIndexesById {
		indexIds = append(indexIds, id)
		indexNames = append(indexNames, index.Name())
		allIndexes = append(allIndexes, index)
		mapIndexesByName[index.Name()] = index
	}

	i.m.Lock()
	i.indexIds = indexIds
	i.indexNames = indexNames
	i.allIndexes = allIndexes
	i.mapIndexesById = mapIndexesById
	i.mapIndexesByName = mapIndexesByName
	i.m.Unlock()

	return nil
}

func (i *FTSIndexer) getLatestIndexSet() (map[string]datastore.Index, error) {
	ftsEndpoints := i.agent.FtsEps()

	if len(ftsEndpoints) == 0 {
		return nil, fmt.Errorf("no fts nodes found")
	}

	now := time.Now().UnixNano()
	for k := 0; k < len(ftsEndpoints); k++ {
		indexDefs, err := i.retrieveIndexDefs(
			ftsEndpoints[(now+int64(k))%int64(len(ftsEndpoints))])
		if err == nil {
			return i.convertIndexDefs(indexDefs)
		}
	}

	return nil, fmt.Errorf("could not fetch index defintions from any of the"+
		" known nodes: %v", ftsEndpoints)
}

func (i *FTSIndexer) retrieveIndexDefs(node string) (*cbgt.IndexDefs, error) {
	httpClient := i.agent.HttpClient()
	if httpClient != nil {
		return nil, fmt.Errorf("retrieveIndexDefs, client not available")
	}

	resp, err := httpClient.Get(node + "/api/index")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("retrieveIndexDefs, resp status code: %v",
			resp.StatusCode)
	}

	bodyBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var body struct {
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
		Status    string          `json:"status"`
	}
	err = json.Unmarshal(bodyBuf, &body)
	if err != nil {
		return nil, err
	}

	if body.Status != "ok" || body.IndexDefs == nil {
		return nil, fmt.Errorf("retrieveIndexDefs status error,"+
			" body: %+v, bodyBuf: %v", body, bodyBuf)
	}

	return body.IndexDefs, nil
}

// Convert FTS index definitions into a map of n1ql index id mapping to
// datastore.FTSIndex
func (i *FTSIndexer) convertIndexDefs(indexDefs *cbgt.IndexDefs) (
	map[string]datastore.Index, error) {
	rv := map[string]datastore.Index{}

	for _, indexDef := range indexDefs.IndexDefs {
		fieldTypeMap := map[string][]string{}

		bp := cbft.NewBleveParams()
		err := json.Unmarshal([]byte(indexDef.Params), bp)
		if err != nil {
			logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" json unmarshal indexDef.Params, err: %v\n", indexDef, err)
			continue
		}

		if bp.DocConfig.Mode != "type_field" {
			logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" wrong DocConfig.Mode\n", indexDef)
			continue
		}

		typeField := bp.DocConfig.TypeField
		if typeField == "" {
			logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" wrong DocConfig.TypeField\n", typeField)
			continue
		}

		bm, ok := bp.Mapping.(*mapping.IndexMappingImpl)
		if !ok {
			logging.Infof("n1fty: convertIndexDefs skip indexDef: %+v, "+
				" not IndexMappingImpl\n", *indexDef)
			continue
		}

		for typeName, typeMapping := range bm.TypeMapping {
			if typeMapping.Enabled {
				if typeMapping.Dynamic {
					// everything under document type is indexed
					fieldTypeMap[typeName] = []string{"_all"}
				} else {
					rv := fetchCompleteFieldNames("", typeMapping)
					fieldTypeMap[typeName] = rv
				}
			}
		}

		if bm.DefaultMapping != nil && bm.DefaultMapping.Enabled {
			if bm.DefaultMapping.Dynamic {
				fieldTypeMap["default"] = []string{"_all"}
			} else {
				rv := fetchCompleteFieldNames("", bm.DefaultMapping)
				fieldTypeMap["default"] = rv
			}
		}

		rv[indexDef.Name], err = newFTSIndex(fieldTypeMap, indexDef, i)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

// -----------------------------------------------------------------------------

func fetchCompleteFieldNames(path string, typeMapping *mapping.DocumentMapping) []string {
	rv := []string{}
	for _, field := range typeMapping.Fields {
		if field.Index {
			if len(path) == 0 {
				rv = append(rv, field.Name)
			} else {
				rv = append(rv, path+"."+field.Name)
			}
		}
	}

	for childMappingName, childMapping := range typeMapping.Properties {
		newPath := path
		if len(childMapping.Fields) == 0 {
			if len(path) == 0 {
				newPath += childMappingName
			} else {
				newPath += "." + childMappingName
			}
		}
		if typeMapping.Enabled {
			if typeMapping.Dynamic {
				rv = append(rv, newPath)
			} else {
				extra := fetchCompleteFieldNames(newPath, childMapping)
				rv = append(rv, extra...)
			}
		}
	}

	return rv
}
