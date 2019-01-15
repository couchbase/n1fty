// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

// client is the integration of N1ql with FTs Yndexing.

package client

import (
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"google.golang.org/grpc"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/gocb"

	gocbCBFT "github.com/couchbase/gocb/cbft"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
	"google.golang.org/grpc/keepalive"
	log "github.com/couchbase/clog"
	pb "github.com/couchbase/cbft/protobuf"
)

// MetaDataRefreshDurationMsec is the default time in
// msecs to cache a previous metadata refresh.
var MetaDataRefreshDurationMsec = 5

var IndexerProviderName = "fts"

const N1ftyBackfillPrefix = "search-results"

var HttpGet func(url string) (resp *http.Response, err error) = http.Get

// An indexer implements the n1ql datastore.Indexer interface, and
// focuses on just FTS indexes.
type indexer struct {
	clusterURL string
	namespace  string // aka pool
	keyspace   string // aka bucket

	// The startTime is also used to target each indexer at a
	// different ftsEndpoint.
	startTime time.Time

	logPrefix string

	cluster *gocb.Cluster

	rw sync.RWMutex // Protects the fields that follow.

	lastRefreshStartTime time.Time

	// The following fields are immutable, and must be cloned on
	// "mutation".  The following should either be all nil (when no
	// buckets or no FTS services), or all non-nil together.

	indexIds   []string // All index id's.
	indexNames []string // All index names.

	allIndexes []datastore.Index        // []*index
	priIndexes []datastore.PrimaryIndex // []*index

	mapIndexesById   map[string]*index
	mapIndexesByName map[string]*index

	stats  *n1ftyStats
	config *n1ftyConfig
}

// An index implements the n1ql datastore.Index interface,
// representing a part of an FTS index.  One FTS index can result in 1
// or more datastore.Index instances.  This is because an FTS index
// defines a mapping that can contain more than one field.
//
// The fields of an index are immutable.
type index struct {
	indexer *indexer
	id      string
	name    string

	indexDef *cbgt.IndexDef

	typeField string

	fieldPath   string // Ex: "locations.address.city".
	indexFields []*indexField

	seekKeyExprs  expression.Expressions
	rangeKeyExprs expression.Expressions
	conditionExpr expression.Expression

	isPrimary bool

	backfill *os.File
}

type queryHandler struct {
	i         *index
	requestID string
	backfills *os.File
}

type n1ftyStats struct {
	BackfillLimit   int64
	CurBackfillSize int64
	TotalScans      int64
	BlockedDuration int64
	ScanDuration    int64
}

// An indexField represents a bleve index field.
type indexField struct {
	docType   string
	isDefault bool // True when docType is for the default mapping.
	field     *mapping.FieldMapping
}

// ----------------------------------------------------

// NewFTSIndexer manage a set of indexes under namespace->keyspace,
// also called as, pool->bucket. It will return an error when,
// - FTS cluster is not available.
// - network partitions / errors.
func NewFTSIndexer(clusterURL, namespace, keyspace string) (
	datastore.Indexer, errors.Error) {
	log.Printf("n1fty: clusterURL: %s, namespace: %s, keyspace: %s",
		clusterURL, namespace, keyspace)
	cluster, err := gocb.Connect(clusterURL)
	if err != nil {
		log.Printf("n1fty: clusterURL: %s, namespace: %s, keyspace: %s, err: %v",
			clusterURL, namespace, keyspace, err)
		return nil, errors.NewError(err, "n1fty: gocb.Connect")
	}

	// TODO: Need to configure cluster authenticator?
	//bucketMap := make(map[string]gocb.BucketAuthenticator, 1)
	//bucketMap["travel-sample"] = gocb.BucketAuthenticator{Password: "asdasd"}

	cluster.Authenticate(gocb.PasswordAuthenticator{ // TODO.
		Username: "Administrator",
		Password: "asdasd",
	})

	indexer := &indexer{
		clusterURL: clusterURL,
		namespace:  namespace,
		keyspace:   keyspace,
		startTime:  time.Now(),
		cluster:    cluster,
		stats:      &n1ftyStats{},
	}

	err = indexer.Refresh()
	if err != nil {
		return nil, errors.NewError(err, "n1fty: refresh err")
	}

	go backfillMonitor(2*time.Second, indexer)

	return indexer, nil
}

// ----------------------------------------------------

func (indexer *indexer) MetadataVersion() uint64 {
	return 0 // placeholder
}

func (indexer *indexer) KeyspaceId() string {
	return indexer.keyspace
}

func (indexer *indexer) Name() datastore.IndexType {
	return datastore.IndexType(IndexerProviderName)
}

// IndexIds returns the Ids of the current set of FTS indexes.
func (indexer *indexer) IndexIds() ([]string, errors.Error) {
	if err := indexer.maybeRefresh(false); err != nil {
		return nil, err
	}

	indexer.rw.RLock()
	indexIds := indexer.indexIds
	indexer.rw.RUnlock()

	return indexIds, nil
}

// IndexNames returns the names of the current set of FTS indexes.
func (indexer *indexer) IndexNames() ([]string, errors.Error) {
	if err := indexer.maybeRefresh(false); err != nil {
		return nil, err
	}

	indexer.rw.RLock()
	indexNames := indexer.indexNames
	indexer.rw.RUnlock()

	return indexNames, nil
}

// IndexById finds an index by id.
func (indexer *indexer) IndexById(id string) (datastore.Index, errors.Error) {
	var index datastore.Index
	var ok bool

	indexer.rw.RLock()
	if indexer.mapIndexesById != nil {
		index, ok = indexer.mapIndexesById[id]
	}
	indexer.rw.RUnlock()

	if !ok {
		return nil, errors.NewError(nil,
			fmt.Sprintf("fts index with id %v not found", id))
	}

	return index, nil
}

// IndexByName finds an index by name.
func (indexer *indexer) IndexByName(name string) (datastore.Index, errors.Error) {
	var index datastore.Index
	var ok bool

	indexer.rw.RLock()
	if indexer.mapIndexesByName != nil {
		index, ok = indexer.mapIndexesByName[name]
	}
	indexer.rw.RUnlock()

	if !ok {
		return nil, errors.NewError(nil,
			fmt.Sprintf("fts index named %v not found", name))
	}

	return index, nil
}

// Indexes returns the latest set of all fts indexes.
func (indexer *indexer) Indexes() ([]datastore.Index, errors.Error) {
	if err := indexer.maybeRefresh(false); err != nil {
		return nil, err
	}

	indexer.rw.RLock()
	allIndexes := indexer.allIndexes
	indexer.rw.RUnlock()

	return allIndexes, nil
}

// PrimaryIndexes returns the latest set of fts primary indexes.
func (indexer *indexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	// placeholder
	return nil, errors.NewError(nil, "unimplemented")
}

// ----------------------------------------------------

// CreatePrimaryIndex create or return a primary index on this keyspace.
func (indexer *indexer) CreatePrimaryIndex(requestId, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, errors.NewError(nil, "unimplemented")
}

// CreateIndex create or return an index on this keyspace.
func (indexer *indexer) CreateIndex(
	requestId, name string, seekKey, rangeKey expression.Expressions,
	where expression.Expression, with value.Value) (
	datastore.Index, errors.Error) {
	return nil, errors.NewError(nil, "unimplemented")
}

// BuildIndexes build indexes that were deferred at creation.
func (indexer *indexer) BuildIndexes(
	requestId string,
	name ...string) errors.Error {
	return errors.NewError(nil, "unimplemented") 
}

// Refresh list of indexes from metadata.
func (indexer *indexer) Refresh() errors.Error {
	return indexer.maybeRefresh(true)
}

// Refresh list of indexes from metadata if MetaDataRefreshDurationMsec was
// reached or if force'd.
func (indexer *indexer) maybeRefresh(force bool) (err errors.Error) {
	dur := time.Duration(MetaDataRefreshDurationMsec) * time.Millisecond
	log.Printf("n1fty: maybeRefresh indexer: %+v, force: %v",
		indexer, force)
	
	now := time.Now()
	indexer.rw.Lock()
	if force || indexer.lastRefreshStartTime.IsZero() ||
		now.Sub(indexer.lastRefreshStartTime) > dur {
		force = true

		indexer.lastRefreshStartTime = now
	}
	indexer.rw.Unlock()

	if !force {
		return nil
	}

	mapIndexesById, err := indexer.refresh()
	if err != nil {
		return err
	}

	// process the mapIndexesById into read-friendly format.

	indexNum := len(mapIndexesById)

	indexIds := make([]string, 0, indexNum)
	indexNames := make([]string, 0, indexNum)

	allIndexes := make([]datastore.Index, 0, indexNum)
	priIndexes := make([]datastore.PrimaryIndex, 0, indexNum)

	mapIndexesByName := map[string]*index{}

	for indexId, index := range mapIndexesById {
		indexIds = append(indexIds, indexId)
		indexNames = append(indexNames, index.Name())

		allIndexes = append(allIndexes, index)

		// if index.isPrimary { 
		//     priIndexes = append(priIndexes, index)
		// }

		mapIndexesByName[index.Name()] = index
		log.Printf("index.Name() %s set", index.Name())
	}

	indexer.rw.Lock()
	indexer.indexIds = indexIds
	indexer.indexNames = indexNames
	indexer.allIndexes = allIndexes
	indexer.priIndexes = priIndexes
	indexer.mapIndexesById = mapIndexesById
	indexer.mapIndexesByName = mapIndexesByName
	indexer.rw.Unlock()

	return nil
}

func (indexer *indexer) refresh() (map[string]*index, errors.Error) {
	ftsEndpoints, err := indexer.ftsEndpoints()
	if err != nil {
		log.Printf("n1fty: refresh ftsEndpoints, err: %v", err)
		return nil, errors.NewError(err, "refresh ftsEndpoints")
	}

	if len(ftsEndpoints) <= 0 {
		return nil, errors.NewError(err, "no fts nodes found")
	}

	var errSummary string
	for i := 0; i < len(ftsEndpoints); i++ {
		x := (int(indexer.startTime.UnixNano()) + i) % len(ftsEndpoints)

		indexDefs, err := indexer.retrieveIndexDefs(ftsEndpoints[x])
		if err == nil {
			return indexer.convertIndexDefs(indexDefs), nil
		} 
		errSummary += "\t"+ err.Error()
		log.Printf("n1fty: retrieveIndexDefs, ftsEndpoints: %s,"+
			" indexer: %v, err: %v", ftsEndpoints[x], indexer, err)	
	}

	return nil, errors.NewError(fmt.Errorf("fts service error"), errSummary)
}

// Returns array of FTS endpoint strings that look like
// "http(s)://HOST:PORT".
func (indexer *indexer) ftsEndpoints() (rv []string, err error) {
	user, pswd := "", ""                   // Forces usage of auth manager.
	user, pswd = "Administrator", "asdasd" // TODO.

	buckets, err := indexer.cluster.Manager(user, pswd).GetBuckets()
	if err != nil {
		return nil, err
	}

	if len(buckets) <= 0 {
		return nil, nil
	}

	bucket, err := indexer.cluster.OpenBucket(buckets[0].Name, "")
	if err != nil {
		log.Printf("n1fty: openBucket err: %v", err)
		return nil, err
	}

	ioRouter := bucket.IoRouter()
	if ioRouter != nil {
		rv = ioRouter.FtsEps()
	}
	bucket.Close()

	return rv, nil
}

// Retrieve the index definitions from an FTS endpoint.
func (indexer *indexer) retrieveIndexDefs(ftsEndpoint string) (
	*cbgt.IndexDefs, error) {
	ftsEndpoint = strings.Replace(ftsEndpoint,
		"http://", "http://Administrator:asdasd@", 1) // TODO. cbauth for default auths

	resp, err := HttpGet(ftsEndpoint + "/api/index") // TODO: Auth.
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("retrieveIndexDefs resp: %#v", resp)
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
			" body: %+v, bodyBuf: %s", body, bodyBuf)
	}
	
	return body.IndexDefs, nil
}

// Convert an FTS index definitions into a map[n1qlIndexId]*index.
func (indexer *indexer) convertIndexDefs(
	indexDefs *cbgt.IndexDefs) map[string]*index {

	rv := map[string]*index{}

OUTER:
	for _, indexDef := range indexDefs.IndexDefs {

		if indexDef.Type != "fulltext-index" {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" not a fulltext-index", indexDef.Type)
			continue
		}

		if indexDef.SourceType != "couchbase" &&
			indexDef.SourceType != "couchbase-dcp" {
				log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" not couchbase/couchbase-dcp SourceType", indexDef.SourceType)
			continue
		}

		/*if indexDef.SourceName != indexer.keyspace {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" SourceName != keyspace: %s", indexDef.SourceName, 
				indexer.keyspace)
			continue
		}*/

		bp := cbft.NewBleveParams()

		err := json.Unmarshal([]byte(indexDef.Params), bp)
		if err != nil {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" json unmarshal indexDef.Params, err: %v", indexDef, err)
			continue
		}

		if bp.DocConfig.Mode != "type_field" {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" wrong DocConfig.Mode", indexDef)
			continue
		}

		typeField := bp.DocConfig.TypeField
		if typeField == "" {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" wrong DocConfig.TypeField", typeField)
			continue
		}

		bm, ok := bp.Mapping.(*mapping.IndexMappingImpl)
		if !ok {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v, "+
				" not IndexMappingImpl", *indexDef)
			continue
		}
		log.Printf("n1fty: bm.TypeMapping : %+v", bm.TypeMapping)

		// Keyed by field path (ex: "name", "address.geo.lat").
		fieldPathIndexFields := map[string][]*indexField{}
		var indexesWithDefaultMapping []string
		if len(bm.TypeMapping) > 0 {
			// If we have type mappings, then we can't support any
			// default mapping.
			if bm.TypeMapping[bm.DefaultType] != nil {
				log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
					" TypeMapping[bm.DefaultType] non-nil", 
					bm.TypeMapping[bm.DefaultType])
				continue
			}

			/*if bm.DefaultMapping != nil &&
				bm.DefaultMapping.Enabled {
				if bm.DefaultMapping.Dynamic ||
					len(bm.DefaultMapping.Properties) > 0 ||
					len(bm.DefaultMapping.Fields) > 0 {
					log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
						" default mapping exists when type mappings exist", indexDef)
					//continue // TODO: Handle default mapping one day.
				}
			}*/

			for t, m := range bm.TypeMapping {
				allowed := indexer.convertDocMap(t, false, "", "",
					m, fieldPathIndexFields)
				if !allowed {
					log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
						" convertDocMap disallowed for type: %v, m: %v",
						indexDef, t, m)
					continue OUTER
				}
			}
		} else {
			// if we have default type mappings,
			if bm.DefaultMapping != nil &&
				bm.DefaultMapping.Enabled == true &&
				bm.DefaultMapping.Dynamic {
					if len(bm.DefaultMapping.Properties) == 0 &&
						len(bm.DefaultMapping.Fields) == 0 {
						indexesWithDefaultMapping = 
							append(indexesWithDefaultMapping,indexDef.Name)
					}
			}

			defaultDocMap := bm.TypeMapping[bm.DefaultType]
			if defaultDocMap == nil {
				defaultDocMap = bm.DefaultMapping
			}
			if defaultDocMap == nil {
				//continue
			}
			log.Printf("defaultDocMap %+v", *defaultDocMap)

			allowed := indexer.convertDocMap("", true, "", "",
				defaultDocMap, fieldPathIndexFields)
			if !allowed {
				log.Printf("reached here fieldPathIndexFields: %+v", fieldPathIndexFields)
				continue
			}

			
		}

		log.Printf("\n before fieldPathIndexFields %+v", fieldPathIndexFields)

		tempMap := make(map[string][]*indexField, 1)
		// add extra entries for fieldPath suffixed with the analyser name
		// to later help the query serving capabilities at client side
		for fieldPath, indexFields := range fieldPathIndexFields {
			for _, indexField := range indexFields {
				if indexField.field.Analyzer != "" {
					newPath := fieldPath + "/" + indexField.field.Analyzer
					tempMap[newPath] = append(tempMap[newPath], indexField) 
				}
			}
		}

		if len(tempMap) > 0 {
			for k,v := range tempMap {
				fieldPathIndexFields[k] = v
			}
		}

		log.Printf("\n after fieldPathIndexFields %+v", fieldPathIndexFields)

		// Generate index metadata for each fieldPath.
		//
		for fieldPath, indexFields := range fieldPathIndexFields {
			index, err := makeIndex(fieldPath, typeField, indexFields, indexer, indexDef)
			if err != nil {
				continue OUTER
			}
			rv[index.id] = index
		}

	
	}

	for k, v:= range rv {
		log.Printf("\n key %s val %+v", k, *v)
	}
	
	return rv
}

func makeIndex(fieldPath, typeField string, indexFields []*indexField, 
	indexer *indexer, indexDef *cbgt.IndexDef) (*index, error) {
	fieldPathS := fmt.Sprintf("`%s`.`%s`", indexer.keyspace, fieldPath)

	rangeKeyExpr, err := parser.Parse(fieldPathS)
	if err != nil {
		log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
			" parse fieldPath: %v, err: %v",
			indexDef, fieldPath, err)
		return nil, err
	}

	indexName := "__FTS__/" + indexDef.Name + "/" + fieldPath 

	index := &index{
		indexer:       indexer,
		id:            indexName + "/" + indexDef.UUID,
		name:          indexName,
		indexDef:      indexDef,
		typeField:     typeField,
		fieldPath:     fieldPath,
		indexFields:   indexFields,
		seekKeyExprs:  expression.Expressions{},
		rangeKeyExprs: expression.Expressions{rangeKeyExpr},
	}

	conditionExprs := make([]string, 0, len(indexFields))
	for _, indexField := range indexFields {
		if !indexField.isDefault {
			conditionExprs = append(conditionExprs,
				fmt.Sprintf("`%s` = %q", typeField, indexField.docType))
		}
	}

	if len(conditionExprs) > 0 {
		conditionExpr, err :=
			parser.Parse(strings.Join(conditionExprs, " OR "))
		if err != nil {
			log.Printf("n1fty: convertIndexDefs skip indexDef: %+v,"+
				" parse conditionExprs: %+v, err: %v",
				indexDef, conditionExprs, err)
			return nil, err
		}

		index.conditionExpr = conditionExpr
	}
	return index, nil
}

// ------------------------------------------------

// convertDocMap() recursively processes the parts of a
// mapping.DocumentMapping into one or more N1QL datastore.Index
// metadata instances.
//
// The docType might be nil when it's the default docMapping.
//
// Returns true if the conversion should continue for the entire
// indexDef, or false if conversion should stop (such as due to
// dynamic mapping).
//
// TODO: We currently ignore the _all / IncludeInAll feature.
func (indexer *indexer) convertDocMap(docType string, isDefault bool,
	docMapPath, docMapName string, docMap *mapping.DocumentMapping,
	rv map[string][]*indexField) bool {
	if docMap == nil || !docMap.Enabled {
		return true
	}

	if docMap.Dynamic {
		// Any dynamic seen anywhere means disallow the conversion of
		// the indexDef.
		return false
	}

	for _, field := range docMap.Fields {
		if field.Index == false {
			continue
		}

		if docMapName != "" && docMapName != field.Name {
			return false
		}

		rv[docMapPath] = append(rv[docMapPath], &indexField{
			docType:   docType,
			isDefault: isDefault,
			field:     field,
		})
	}

	for childName, childMap := range docMap.Properties {
		childPath := docMapPath
		if len(childPath) > 0 {
			childPath += "."
		}
		childPath += childName

		allowed := indexer.convertDocMap(docType, isDefault,
			childPath, childName, childMap, rv)
		if !allowed {
			return false
		}
	}

	return true
}

// ------------------------------------------------

// Set log level for in-process logging.
func (indexer *indexer) SetLogLevel(level logging.Level) {
	// TODO.
}

// ------------------------------------------------

func (i *index) Indexer() datastore.Indexer {
	return i.indexer
}

// Id of the keyspace to which this index belongs.
func (i *index) KeyspaceId() string {
	return i.indexer.keyspace
}

// Id of this index.
func (i *index) Id() string {
	return i.name
}

// Name of this index.
func (i *index) Name() string {
	return i.name
}

// Type of this index.
func (i *index) Type() datastore.IndexType {
	return datastore.IndexType(IndexerProviderName)
}

// Equality keys.
func (i *index) SeekKey() expression.Expressions {
	return i.seekKeyExprs
}

// Range keys.
func (i *index) RangeKey() expression.Expressions {
	return i.rangeKeyExprs
}

// Condition, if any.
func (i *index) Condition() expression.Expression {
	return i.conditionExpr
}

// Is this a primary index?
func (i *index) IsPrimary() bool {
	return i.isPrimary
}

// Obtain state of this index.
func (i *index) State() (state datastore.IndexState, msg string, err errors.Error) {
	return datastore.ONLINE, "", nil
}

// Obtain statistics for this index.
func (i *index) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return &indexStatistics{index: i}, nil
}

// Drop / delete this index.
func (i *index) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "unimplemented")
}

func cleanupBackfills(tmpfile *os.File, requestID string) {
	if tmpfile != nil {
		tmpfile.Close()
		fname := tmpfile.Name()
		fmsg := "request(%v) removing backfill file %v ...\n"
		err := fmt.Errorf(fmsg, requestID, fname)
		log.Printf("\ncleaning files -> %v", err)
		if err := os.Remove(fname); err != nil {
			fmsg := "%v remove backfill file %v unexpected failure: %v\n"
			fmt.Errorf(fmsg, fname, err)
		}
	}
}

const DONEREQUEST = int64(1)

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	auth := b.username + ":" + b.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{
		"authorization": "Basic " + enc,
	}, nil
}

func (basicAuth) RequireTransportSecurity() bool {
	return false
}

// Perform a scan on this index. Distinct and limit are hints.
func (i *index) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	starttm := time.Now()
	entryCh := conn.EntryChannel()

	var waitGroup sync.WaitGroup
	var backfillSync int64
	var qh *queryHandler

	defer func() { // cleanup tmpfile
		atomic.StoreInt64(&backfillSync, DONEREQUEST)
		//log.Printf("SCAN DONEREQUEST, Awaiting..")
		waitGroup.Wait()
		close(entryCh)
		cleanupBackfills(qh.backfills, requestId)
	}()

	// TODO. handle distinct.
	// TODO. construct bquery and do something with span.
	// TODO. do something with vector and scan consistency.

	// TODO: We currently only support exact equals scan.
	if len(span.Range.Low) != 1 ||
		len(span.Range.High) != 1 ||
		span.Range.Low[0].EquivalentTo(span.Range.High[0]) == false ||
		span.Range.Inclusion != datastore.BOTH {
		conn.Error(errors.NewError(nil,
			"n1fty currently implements only exact match span"))
		return
	}

	//bucketPswd := "" // TODO.

	/*bucket, err := i.indexer.cluster.OpenBucket(i.indexer.keyspace, bucketPswd)
	if err != nil {
		conn.Error(errors.NewError(err, "fts ExecuteSearchQuery OpenBucket"))
		return
	}
	defer bucket.Close()
	*/
	term := span.Range.Low[0].String() // Enclosed with double-quotes.
	if term[0] == '"' && term[len(term)-1] == '"' {
		term = term[1 : len(term)-1]
	}

	logging.Infof("fts index.Scan, index.id: %#v, requestId: %s, term: %s",
		i.id, requestId, term)

	tquery := gocbCBFT.NewTermQuery(term).Field(i.fieldPath)

	squery := gocb.NewSearchQuery(i.indexDef.Name, tquery)

	limiti := int(limit)
	if limiti > 10000 {
		limiti = 10000 // TODO.
	}
	squery.Limit(limiti)

	//sresults, err := bucket.ExecuteSearchQuery(squery)
	address := "localhost:15000"
	opts := []grpc.DialOption{

		grpc.WithInsecure(),

		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Send keepalive every 60 seconds to prevent the
			// connection from getting closed by upstreams
			Time: time.Duration(60) * time.Second,
		}),

		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*2000),
			grpc.MaxCallSendMsgSize(1024*1024*20),
		),


		grpc.WithPerRPCCredentials(&basicAuth{
			username: "Administrator",
			password: "asdasd",
		},
		),
	}

	// Set up a connection to the gRPC server.
	grpcConn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer grpcConn.Close()
	// Creates a new CustomerClient
	client := pb.NewSearchSrvClient(grpcConn)

	// send a stream search request
	req1 := &pb.SearchRequest{Query: []byte(`{"query": "country:\"United States\""}`), Stream: true, Size: int64(limit), From: int64(0), IndexName: "FTS"}

	stream, err := client.Search(context.Background(), req1)
	if err != nil || stream == nil {
		log.Printf("client: Search failed, err: %v", err)
		return
	}

	qh = &queryHandler{requestID: requestId, i: i}

	qh.handleStreamResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalScans, 1)
	atomic.AddInt64(&i.indexer.stats.ScanDuration, int64(time.Since(starttm)))
}

func (qh *queryHandler) handleStreamResponse(conn *datastore.IndexConnection,
	waitGroup *sync.WaitGroup,
	backfillSync *int64,
	stream pb.SearchSrv_SearchClient) {

	entryCh := conn.EntryChannel()
	backfillLimit := qh.i.getTmpSpaceLimit()
	//primed, starttm, ticktm := false, time.Now(), time.Now()

	log.Printf("handleStreamResponse started")
	var enc *gob.Encoder
	var dec *gob.Decoder
	var readfd *os.File

	logPrefix := fmt.Sprintf("N1FTY[%s/%s-%v]", qh.i.Name(), qh.i.KeyspaceId(), time.Now().UnixNano())
	var tmpfile *os.File
	var backfillFin, backfillEntries int64

	backfill := func() {
		name := tmpfile.Name()
		defer func() {
			if readfd != nil {
				readfd.Close()
			}
			waitGroup.Done()
			atomic.AddInt64(&backfillFin, 1)
			log.Printf(
				"%v %q finished backfill for %v ...\n",
				logPrefix, qh.requestID, name)

			recover() // need this because entryChannel() would have closed
		}()

		log.Printf("%v %q started backfill for %v ...\n", logPrefix, qh.requestID, name)

		for {
			if pending := atomic.LoadInt64(&backfillEntries); pending > 0 {
				atomic.AddInt64(&backfillEntries, -1)
			} else if done := atomic.LoadInt64(backfillSync); done == DONEREQUEST {
				log.Printf("DONEREQUEST\n")
				return
			} else {
				// wait a bit
				time.Sleep(1 * time.Millisecond)
				continue
			}

			cummsize := float64(atomic.LoadInt64(&qh.i.indexer.stats.CurBackfillSize)) / (1024 * 1024) // make constant
			log.Printf("atomic.LoadInt64(&i.indexer.stats.CurBackfillSize) %d cummsize %f", atomic.LoadInt64(&qh.i.indexer.stats.CurBackfillSize), cummsize)
			if cummsize > float64(backfillLimit) {
				fmsg := "%q backfill exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, qh.requestID, backfillLimit, cummsize)
				conn.Error(n1qlError(err))
				log.Printf("cummsize %f exceeds limit %d", cummsize, backfillLimit)
				return
			}

			skeys := make([]interface{}, 0)
			if err := dec.Decode(&skeys); err != nil {
				fmsg := "%v %q decoding from backfill %v: %v\n"
				err := fmt.Errorf(fmsg, logPrefix, qh.requestID, name, err)
				conn.Error(n1qlError(err))

				log.Printf("skeys decode err: %v", err)

				return
			}
			/*pkeys := make([][]byte, 0)
			if err := dec.Decode(&pkeys); err != nil {
				fmsg := "%v %q decoding from backfill %v: %v\n"
				fmt.Errorf(fmsg, logPrefix, requestId, name, err)
				conn.Error(n1qlError(err))
				log.Printf("pkeys decode err: %v", err)
				//broker.Error(err, instId, partitions)
				return
			}*/

			log.Printf("%v backfill read %v entries\n", logPrefix, len(skeys))

			//var vals []value.Value
			//var bKey *[]byte
			for _, skey := range skeys {
				bKey, ok := skey.([]byte)
				if !ok {
					log.Printf("byte err: %v", ok)
				}
				qh.sendEntry(string(bKey), nil, conn)
			}

			/*if primed == false {
				//atomic.AddInt64(&si.gsi.primedur, int64(time.Since(starttm)))
				primed = true
			}

			ln := int(broker.Len(id))
			if ln < 0 {
				ln = len(entryChannel)
			}

			if ln > 0 && len(skeys) > 0 {
				atomic.AddInt64(&si.gsi.throttledur, int64(time.Since(ticktm)))
			}
			if !broker.SendEntries(id, pkeys, skeys) {
				return
			}
			ticktm = time.Now()
			*/

		}
	}

	for {
		results, err := stream.Recv()
		if err == io.EOF {
			log.Printf("EOF")
			// return as it read all data
			return
		}

		if err != nil {
			log.Printf("stream read err: %v", err)
			conn.Error(n1qlError(err))
			
		}

		if results.GetHits() != nil {
			var hits []*search.DocumentMatch
			switch r := results.PayLoad.(type) {

			case *pb.StreamSearchResults_Hits:
				err = json.Unmarshal(r.Hits, &hits)
				if err != nil {
					log.Printf("client: stream recv failed, err: %v", err)
				}
			}

			log.Printf("batch size = %d", len(hits))

			ln := len(entryCh)
			cp := cap(entryCh)

			if backfillLimit > 0 && tmpfile == nil &&
				((cp - ln) < len(hits)) {
				log.Printf("Buffer outflow observed!!  cap %d len %d", cp, ln)
				enc, dec, tmpfile, err = initBackFill(logPrefix, qh.requestID, qh)
				if err != nil {
					conn.Error(n1qlError(err))
					return
				}
				waitGroup.Add(1)
				go backfill()
			}

			// slow reader found and hence start dumping the results to the backfill file
			if tmpfile != nil {
				// whether temp-file is exhausted the limit.
				cummsize := float64(atomic.LoadInt64(&qh.i.indexer.stats.CurBackfillSize)) / (1024 * 1024)
				log.Printf("atomic.LoadInt64(&i.indexer.stats.CurBackfillSize) %d cummsize %f", atomic.LoadInt64(&qh.i.indexer.stats.CurBackfillSize), cummsize)
				if cummsize > float64(backfillLimit) {
					fmsg := "%q backfill exceeded limit %v, %v"
					err := fmt.Errorf(fmsg, qh.requestID, backfillLimit, cummsize)
					conn.Error(n1qlError(err))
					log.Printf("cummsize %f exceeds limit %d", cummsize, backfillLimit)
					return
				}

				if atomic.LoadInt64(&backfillFin) > 0 {
					return
				}

				err := writeToBackfill(hits, enc)
				if err != nil {
					log.Printf("writeToBackfill err: %v", err)
					conn.Error(n1qlError(err))
					return
				}

				atomic.AddInt64(&backfillEntries, 1)

			} else if hits != nil {
				log.Printf("sent back %d items", len(hits))
				for _, hit := range hits {
					qh.sendEntry(hit.ID, nil, conn)
				}
			}

		}
	}

}

func (qh *queryHandler) sendEntry(key string, value []byte, conn *datastore.IndexConnection) bool {
	var start time.Time
	blockedtm, blocked := int64(0), false

	entryCh := conn.EntryChannel()
	stopCh := conn.StopChannel()

	cp, ln := cap(entryCh), len(entryCh)
	if ln == cp {
		start, blocked = time.Now(), true
	}

	select {
	case entryCh <- &datastore.IndexEntry{PrimaryKey: key}:
		// NO-OP.
	case <-stopCh:
		return false
	}

	if blocked {
		blockedtm += int64(time.Since(start))
		atomic.AddInt64(&qh.i.indexer.stats.BlockedDuration, blockedtm)
	}

	return true
}

func backfillMonitor(period time.Duration, i *indexer) {
	tick := time.NewTicker(period)
	defer func() {
		tick.Stop()
	}()

	for {
		<-tick.C
		nifty_backfill_temp_dir := getTmpSpaceDir()
		files, err := ioutil.ReadDir(nifty_backfill_temp_dir)
		if err != nil {
			return
		}

		size := int64(0)
		for _, file := range files {
			fname := path.Join(nifty_backfill_temp_dir, file.Name())
			if strings.Contains(fname, N1ftyBackfillPrefix) {
				size += int64(file.Size())
			}
		}
		atomic.StoreInt64(&i.stats.CurBackfillSize, size)
	}
}

func n1qlError(err error) errors.Error {
	return errors.NewError(err /*client.DescribeError(err)*/, "")
}

func initBackFill(logPrefix, requestId string, qh *queryHandler) (*gob.Encoder,
	*gob.Decoder, *os.File, error) {
	prefix := N1ftyBackfillPrefix + strconv.Itoa(os.Getpid())
	tmpfile, err := ioutil.TempFile(getTmpSpaceDir(), prefix)
	name := ""

	if tmpfile != nil {
		name = tmpfile.Name()
		qh.backfills = tmpfile
	}

	if err != nil {
		fmsg := "%v %q creating backfill file %v : %v\n"
		return nil, nil, nil, fmt.Errorf(fmsg, logPrefix, requestId, name, err)
	}

	fmsg := "%v %v new backfill file ... %v\n"
	fmt.Errorf(fmsg, logPrefix, requestId, name)
	// encoder
	enc := gob.NewEncoder(tmpfile)
	readfd, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		fmsg := "%v %v reading backfill file %v: %v\n"
		return nil, nil, tmpfile, fmt.Errorf(fmsg, logPrefix, requestId, name, err)
	}
	log.Printf("!!Started writting to file ...!!!")

	// decoder
	return enc, gob.NewDecoder(readfd), tmpfile, nil
}

func writeToBackfill(hits []*search.DocumentMatch, enc *gob.Encoder) error {
	/*var hits []*pb.DocumentMatch
	if dmc != nil {
		hits = dmc.Hits
	}*/
	var skeys []interface{}
	var pkeys [][]byte
	for _, hit := range hits {
		skeys = append(skeys, []byte(hit.ID))
	}

	//log.Printf("\n  backfill write %d entries\n", len(skeys))

	if err := enc.Encode(skeys); err != nil {
		return err
	}

	if len(pkeys) > 0 {
		if err := enc.Encode(pkeys); err != nil {
			return err
		}
	}

	return nil
}

// TODO: In order to implement datastore.PrimaryIndex interface.
/*
func (i *index) ScanEntries(requestId string,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	close(conn.EntryChannel()) // TODO.
	conn.Error(errors.NewError(nil, "unimplemented"))
}
*/

// TODO: In order to implement datastore.SizedIndex interface.
/*
func (i *index) SizeFromStatistics(requestId string) (
	int64, errors.Error) {
	return 0, nil // TODO.
}
*/

// TODO: In order to implement datastore.CountIndex{} interface.
/*
func (i *index) Count(span *datastore.Span,
	cons datastore.ScanConsistency,
	vector timestamp.Vector) (int64, errors.Error) {
	return 0, nil // TODO.
}
*/

// ------------------------------------------------

type indexStatistics struct {
	index *index
}

func (is *indexStatistics) Count() (int64, errors.Error) {
	return 0, nil // TODO.
}

func (is *indexStatistics) Min() (value.Values, errors.Error) {
	return nil, nil // TODO.
}

func (is *indexStatistics) Max() (value.Values, errors.Error) {
	return nil, nil // TODO.
}

func (is *indexStatistics) DistinctCount() (int64, errors.Error) {
	return 0, nil // TODO.
}

func (is *indexStatistics) Bins() ([]datastore.Statistics, errors.Error) {
	return nil, nil // TODO.
}

//-------------------------------------
// n1ftyConfig Implementation
//-------------------------------------

const n1ftyTmpSpaceDirKey = "n1fty_tmpspace_dir"
const n1ftyTmpSpaceLimitKey = "n1fty_tmpspace_limit"

const n1ftyDefaultBackfillLimit = int64(100)

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
		log.Printf("validateConfig err %v", err)
		return err
	}

	c.processConfig(conf)

	//make local copy so caller caller doesn't accidently modify
	localconf := make(map[string]interface{})
	for k, v := range conf {
		localconf[k] = v
	}

	log.Printf("n1ftyConfig - Setting config %v", conf)
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
		log.Printf("n1ftyConfig - Setting param %v %v", name, val)
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

	if v, ok := conf[n1ftyTmpSpaceDirKey]; ok {
		if _, ok1 := v.(string); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config Key %v Value %v", n1ftyTmpSpaceDirKey, v)
			return errors.NewError(err, err.Error())
		}
	}

	if v, ok := conf[n1ftyTmpSpaceLimitKey]; ok {
		if _, ok1 := v.(int64); !ok1 {
			err := fmt.Errorf("n1fty Invalid Config Key %v Value %v", n1ftyTmpSpaceLimitKey, v)
			return errors.NewError(err, err.Error())
		}
	}

	return nil
}

func (c *n1ftyConfig) processConfig(conf map[string]interface{}) {

	var olddir interface{}
	var newdir interface{}

	if conf != nil {
		newdir, _ = conf[n1ftyTmpSpaceDirKey]
	}

	prevconf := config.getConfig()

	if prevconf != nil {
		olddir, _ = prevconf[n1ftyTmpSpaceDirKey]
	}

	if olddir == nil {
		olddir = getDefaultTmpDir()
	}

	//cleanup any stale files
	if olddir != newdir {
		cleanupTmpFiles(olddir.(string))
		if newdir != nil {
			cleanupTmpFiles(newdir.(string))
		}
	}

	return

}

// best effort cleanup as tmpdir may change during restart
func cleanupTmpFiles(olddir string) {

	/*files, err := ioutil.ReadDir(olddir)
	if err != nil {
		return
	}

	conf, _ := c.GetSettingsConfig(c.SystemConfig)
	scantm := conf["indexer.settings.scan_timeout"].Int() // in ms.

	for _, file := range files {
		fname := path.Join(olddir, file.Name())
		mtime := file.ModTime()
		since := (time.Since(mtime).Seconds() * 1000) * 2 // twice the lng scan
		if (strings.Contains(fname, "scan-backfill") || strings.Contains(fname, BACKFILLPREFIX)) && int(since) > scantm {
			fmsg := "GSI client: removing old file %v last-modified @ %v"
			l.Infof(fmsg, fname, mtime)
			os.Remove(fname)
		}
	}*/

}

func (c *n1ftyConfig) getConfig() map[string]interface{} {
	conf := c.config.Load()
	if conf != nil {
		return conf.(map[string]interface{})
	}

	return nil
}

func getTmpSpaceDir() string {
	conf := config.getConfig()
	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[n1ftyTmpSpaceDirKey]; ok {
		return v.(string)
	}

	return getDefaultTmpDir()
}

func (i *index) getTmpSpaceLimit() int64 {
	conf := config.getConfig()
	if conf == nil {
		log.Printf("\n nil conf")
		return n1ftyDefaultBackfillLimit
	}

	if v, ok := conf[n1ftyTmpSpaceLimitKey]; ok {
		log.Printf("\n v.(int64) %d", v.(int64))
		return v.(int64)
	} else {
		return n1ftyDefaultBackfillLimit
	}
}

func getDefaultTmpDir() string {
	file, err := ioutil.TempFile("" /*dir*/, N1ftyBackfillPrefix)
	if err != nil {
		return ""
	}

	default_temp_dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file.

	log.Printf("default_temp_dir %s", default_temp_dir)
	return default_temp_dir
}
