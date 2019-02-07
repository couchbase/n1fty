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
	"context"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

const doneRequest = int64(1)

// FTSIndex implements datastore.FTSIndex interface
type FTSIndex struct {
	indexer  *FTSIndexer
	id       string
	name     string
	indexDef *cbgt.IndexDef

	searchableFields      map[string]struct{} // map of searchable fields
	defaultMappingDynamic bool
	rangeKeyExpressions   expression.Expressions
}

// -----------------------------------------------------------------------------

func newFTSIndex(searchableFieldsMap map[string][]string,
	defaultMappingDynamic bool,
	indexDef *cbgt.IndexDef,
	indexer *FTSIndexer) (*FTSIndex, error) {
	index := &FTSIndex{
		indexer:               indexer,
		id:                    indexDef.UUID,
		name:                  indexDef.Name,
		indexDef:              indexDef,
		searchableFields:      map[string]struct{}{},
		defaultMappingDynamic: defaultMappingDynamic,
		rangeKeyExpressions:   expression.Expressions{},
	}

	v := struct{}{}

	for _, fields := range searchableFieldsMap {
		for _, entry := range fields {
			index.searchableFields[entry] = v
			rangeKeyExpr, err := parser.Parse(entry)
			if err != nil {
				return nil, err
			}
			index.rangeKeyExpressions = append(index.rangeKeyExpressions,
				rangeKeyExpr)
		}
	}

	return index, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) KeyspaceId() string {
	return i.indexer.KeyspaceId()
}

func (i *FTSIndex) Id() string {
	return i.id
}

func (i *FTSIndex) Name() string {
	return i.name
}

func (i *FTSIndex) Type() datastore.IndexType {
	return datastore.FTS
}

func (i *FTSIndex) Indexer() datastore.Indexer {
	return i.indexer
}

func (i *FTSIndex) SeekKey() expression.Expressions {
	// not supported
	return nil
}

func (i *FTSIndex) RangeKey() expression.Expressions {
	return i.rangeKeyExpressions
}

func (i *FTSIndex) Condition() expression.Expression {
	// WHERE clause stuff, not supported
	return nil
}

func (i *FTSIndex) IsPrimary() bool {
	return false
}

func (i *FTSIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (i *FTSIndex) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, errors.NewError(nil, "not supported yet")
}

func (i *FTSIndex) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "not supported")
}

func (i *FTSIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(errors.NewError(nil, "n1fty doesn't support the Scan API"))
	return
}

// Search performs a search/scan over this index, with provided SearchInfo settings
func (i *FTSIndex) Search(requestId string, searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	if conn == nil {
		return
	}

	if searchInfo == nil || searchInfo.Query == nil {
		conn.Error(errors.NewError(nil, "no search parameters provided"))
		return
	}

	starttm := time.Now()
	entryCh := conn.EntryChannel()

	var waitGroup sync.WaitGroup
	var backfillSync int64
	var rh *responseHandler

	defer func() {
		// cleanup the backfill file
		atomic.StoreInt64(&backfillSync, doneRequest)
		waitGroup.Wait()
		close(entryCh)
		cleanupBackfills(rh.backfillFile, requestId)
	}()

	fieldStr := ""
	if searchInfo.Field != nil {
		fieldStr = searchInfo.Field.Actual().(string)
	}
	optionsStr := ""
	if searchInfo.Options != nil {
		optionsStr = searchInfo.Options.Actual().(string)
	}

	query, err := util.BuildQueryBytes(fieldStr,
		searchInfo.Query.Actual().(string),
		optionsStr)
	if err != nil {
		conn.Error(errors.NewError(err, ""))
		return
	}

	searchRequest := &pb.SearchRequest{
		Query:     query,
		Stream:    true,
		From:      searchInfo.Offset,
		Size:      searchInfo.Limit,
		IndexName: i.name,
	}

	client := i.indexer.srvWrapper.getGrpcClient()

	stream, err := client.Search(context.Background(), searchRequest)
	if err != nil || stream == nil {
		conn.Error(errors.NewError(err, "search failed"))
		return
	}

	rh = &responseHandler{requestID: requestId, i: i}

	rh.handleResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalSearch, 1)
	atomic.AddInt64(&i.indexer.stats.TotalSearchDuration, int64(time.Since(starttm)))
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Sargable(field string, query, options value.Value) (
	int, bool, errors.Error) {
	// TODO: len of supported fields may not be needed?
	if i.defaultMappingDynamic {
		return 0, true, nil
	}

	fieldsToSearch, err := util.FetchFieldsToSearch(field, query.String(),
		options.String())
	if err != nil {
		return 0, false, errors.NewError(err, "")
	}

	for _, field := range fieldsToSearch {
		if _, exists := i.searchableFields[field]; !exists {
			return 0, false, nil
		}
	}

	return 0, true, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Pagination(order []string, offset, limit uint64) bool {
	// FIXME
	return false
}

// -----------------------------------------------------------------------------

// basicAuthCreds is an implementation of credentials.PerRPCCredentials
// that transforms the username and password into a base64 encoded value
// similar to HTTP Basic xxx
type basicAuthCreds struct {
	username string
	password string
}

// GetRequestMetadata sets the value for "authorization" key
func (b *basicAuthCreds) GetRequestMetadata(context.Context, ...string) (
	map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + basicAuth(b.username, b.password),
	}, nil
}

// RequireTransportSecurity should be true as even though the credentials
// are base64, we want to have it encrypted over the wire.
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return false // TODO - make it true
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

// -----------------------------------------------------------------------------

func getBackfillSpaceDir() string {
	conf := config.GetConfig()

	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[backfillSpaceDir]; ok {
		return v.(string)
	}

	return getDefaultTmpDir()
}

func getBackfillSpaceLimit() int64 {
	conf := config.GetConfig()

	if conf == nil {
		return defaultBackfillLimit
	}

	if v, ok := conf[backfillSpaceLimit]; ok {
		return v.(int64)
	}

	return defaultBackfillLimit
}
