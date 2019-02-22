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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
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

	// map of SearchFields to dynamic-ness
	searchFields map[util.SearchField]bool
	// true if a top-level dynamic mapping exists on index
	dynamicMapping bool
	// default analyzer
	defaultAnalyzer string
}

// -----------------------------------------------------------------------------

func newFTSIndex(searchFieldsMap map[util.SearchField]bool,
	dynamicMapping bool,
	defaultAnalyzer string,
	indexDef *cbgt.IndexDef,
	indexer *FTSIndexer) (*FTSIndex, error) {
	index := &FTSIndex{
		indexer:         indexer,
		id:              indexDef.UUID,
		name:            indexDef.Name,
		indexDef:        indexDef,
		searchFields:    searchFieldsMap,
		dynamicMapping:  dynamicMapping,
		defaultAnalyzer: defaultAnalyzer,
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
	// not supported
	return nil
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
	return nil, n1qlError(nil, "Statistics not supported yet")
}

func (i *FTSIndex) Drop(requestId string) errors.Error {
	return n1qlError(nil, "Drop not supported")
}

func (i *FTSIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(n1qlError(nil, "Scan not supported"))
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
		conn.Error(n1qlError(nil, "no search parameters provided"))
		return
	}

	fieldStr := ""
	if searchInfo.Field != nil {
		fieldStr = searchInfo.Field.Actual().(string)
	}

	// this sargable(...) check is to ensure that the query is indeed "sargable"
	// at search time, as when the Sargable(..) API is invoked during the
	// prepare time, the query/options may not have been available.
	_, ok, qBytes, er := i.buildQueryAndCheckIfSargable(
		fieldStr, searchInfo.Query, searchInfo.Options)
	if !ok || er != nil {
		conn.Error(n1qlError(er.Cause(), "not sargable"))
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
		rh.cleanupBackfill()
	}()

	searchRequest := &pb.SearchRequest{
		Query:     qBytes,
		Stream:    true,
		From:      searchInfo.Offset,
		Size:      searchInfo.Limit,
		IndexName: i.name,
	}

	client := i.indexer.srvWrapper.getGrpcClient()

	stream, err := client.Search(context.Background(), searchRequest)
	if err != nil || stream == nil {
		conn.Error(n1qlError(err, "search failed"))
		return
	}

	rh = &responseHandler{requestID: requestId, i: i}

	rh.handleResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalSearch, 1)
	atomic.AddInt64(&i.indexer.stats.TotalSearchDuration, int64(time.Since(starttm)))
}

// -----------------------------------------------------------------------------

// Sargable checks if the provided request is applicable for the index.
// Return parameters:
// - count:    This is the number of fields whose names along with analyzers
//             matched with that of the index definition
// - sargable: True if all the fields from the query, are supported by the index
// Note: it is possible for the API to return true on sargable, but 0 as count.
// The caller will have to make the decision on which index to choose based
// on the count and the flag returned.
func (i *FTSIndex) Sargable(field string, query, options expression.Expression) (
	int, bool, errors.Error) {
	var queryVal, optionsVal value.Value
	if query != nil {
		queryVal = query.Value()
	}
	if options != nil {
		optionsVal = options.Value()
	}

	count, sargable, _, err := i.buildQueryAndCheckIfSargable(
		field, queryVal, optionsVal)

	return count, sargable, err
}

func (i *FTSIndex) buildQueryAndCheckIfSargable(field string,
	query, options value.Value) (int, bool, []byte, errors.Error) {
	field = util.CleanseField(field)

	var fieldsToSearch []util.SearchField
	var qBytes []byte
	var err error

	if query != nil {
		qBytes, err = util.BuildQueryBytes(field, query)
		if err != nil {
			return 0, false, nil, n1qlError(err, "")
		}

		fieldsToSearch, err = util.FetchFieldsToSearchFromQuery(qBytes)
		if err != nil {
			return 0, false, qBytes, n1qlError(err, "")
		}
		for k := range fieldsToSearch {
			if fieldsToSearch[k].Analyzer == "" {
				fieldsToSearch[k].Analyzer = i.defaultAnalyzer
			}
		}
	} else {
		analyzer := i.defaultAnalyzer
		if analyzerVal, exists := options.Field("analyzer"); exists {
			if val, ok := analyzerVal.Actual().(string); ok {
				analyzer = val
			}
		}
		fieldsToSearch = []util.SearchField{{
			Name:     field,
			Analyzer: analyzer,
		}}
	}

	if i.dynamicMapping {
		return 1, true, qBytes, nil
	}

	var sargableCount int
	for _, f := range fieldsToSearch {
		dynamic, exists := i.searchFields[f]
		if exists && dynamic {
			// if searched field contains nested fields, then this field is not
			// searchable, and the query not sargable.
			return 0, false, qBytes, nil
		}

		if exists {
			sargableCount++
		} else {
			// check if a prefix of this field name is searchable.
			// - (prefix being delimited by ".")
			// e.g.: potential candidates for "reviews.review.content.author" are:
			// - reviews
			// - reviews.review
			// - reviews.review.content
			// .. only if any of the above mappings are dynamic.
			fieldSplitAtDot := strings.Split(f.Name, ".")
			if len(fieldSplitAtDot) <= 1 {
				// not sargable
				return 0, false, qBytes, nil
			}

			var matched bool
			entry := fieldSplitAtDot[0]
			for k := 1; k < len(fieldSplitAtDot); k++ {
				searchField := util.SearchField{
					Name:     entry,
					Analyzer: f.Analyzer,
				}
				if dynamic1, exists1 := i.searchFields[searchField]; exists1 {
					if dynamic1 {
						sargableCount++
						matched = true
						break
					}
				}

				entry += "." + fieldSplitAtDot[k]
			}

			if !matched {
				// not sargable
				return 0, false, qBytes, nil
			}
		}
	}

	return sargableCount, true, qBytes, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Pageable(order []string, offset, limit int64) bool {
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
