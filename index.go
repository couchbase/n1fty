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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
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

	// supported options for ordering
	optionsForOrdering map[string]struct{}
}

// -----------------------------------------------------------------------------

func newFTSIndex(searchFieldsMap map[util.SearchField]bool,
	dynamicMapping bool,
	defaultAnalyzer string,
	indexDef *cbgt.IndexDef,
	indexer *FTSIndexer) (*FTSIndex, error) {
	index := &FTSIndex{
		indexer:            indexer,
		id:                 indexDef.UUID,
		name:               indexDef.Name,
		indexDef:           indexDef,
		searchFields:       searchFieldsMap,
		dynamicMapping:     dynamicMapping,
		defaultAnalyzer:    defaultAnalyzer,
		optionsForOrdering: make(map[string]struct{}),
	}

	v := struct{}{}
	for _, entry := range []string{"score", "score ASC", "score DESC"} {
		index.optionsForOrdering[entry] = v
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
	return nil, util.N1QLError(nil, "Statistics not supported yet")
}

func (i *FTSIndex) Drop(requestId string) errors.Error {
	return util.N1QLError(nil, "Drop not supported")
}

func (i *FTSIndex) Scan(requestId string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(util.N1QLError(nil, "Scan not supported"))
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
		conn.Error(util.N1QLError(nil, "no search parameters provided"))
		return
	}

	fieldStr := ""
	if searchInfo.Field != nil {
		fieldStr = searchInfo.Field.Actual().(string)
	}

	// this sargable(...) check is to ensure that the query is indeed "sargable"
	// at search time, as when the Sargable(..) API is invoked during the
	// prepare time, the query/options may not have been available.
	sargRV := i.buildQueryAndCheckIfSargable(
		fieldStr, searchInfo.Query, searchInfo.Options, nil)
	if sargRV.err != nil || sargRV.count == 0 {
		conn.Error(util.N1QLError(nil, "not sargable"))
		return
	}

	starttm := time.Now()
	sender := conn.Sender()

	var waitGroup sync.WaitGroup
	var backfillSync int64
	var rh *responseHandler

	defer func() {
		// cleanup the backfill file
		atomic.StoreInt64(&backfillSync, doneRequest)
		waitGroup.Wait()
		sender.Close()
		rh.cleanupBackfill()
	}()

	searchRequest := &pb.SearchRequest{
		Query:     sargRV.queryBytes,
		Stream:    true,
		From:      searchInfo.Offset,
		Size:      searchInfo.Limit,
		IndexName: i.name,
	}

	client := i.indexer.srvWrapper.getGrpcClient()

	stream, err := client.Search(context.Background(), searchRequest)
	if err != nil || stream == nil {
		conn.Error(util.N1QLError(err, "search failed"))
		return
	}

	rh = &responseHandler{requestID: requestId, i: i}

	rh.handleResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalSearch, 1)
	atomic.AddInt64(&i.indexer.stats.TotalSearchDuration, int64(time.Since(starttm)))
}

// -----------------------------------------------------------------------------

type sargableRV struct {
	count        int
	indexedCount int64
	queryFields  interface{}
	queryBytes   []byte
	err          errors.Error
}

// Sargable checks if the provided request is applicable for the index.
// Return parameters:
// - sargable_count: This is the number of fields whose names along with
//                   analyzers from the built query matched with that of
//                   the index definition, for now all of query fields or 0.
// - indexed_count:  This is the total number of indexed fields within the
//                   the FTS index.
// - exact:          True if query value available & options unavailable or
//                   options value provided, for now.
// - queryFields:    The custom map of fields/analyzers obtained from the
//                   query for checking it's sargability.
// The caller will have to make the decision on which index to choose based
// on the sargable_count (higher the better), indexed_count (lower the better),
// and exact (if true) returned.
func (i *FTSIndex) Sargable(field string, query,
	options expression.Expression, customFields interface{}) (
	int, int64, bool, interface{}, errors.Error) {
	var queryVal, optionsVal value.Value
	if query != nil {
		queryVal = query.Value()
	}
	if options != nil {
		optionsVal = options.Value()
	}

	exact := (queryVal != nil) && (options == nil || optionsVal != nil)

	rv := i.buildQueryAndCheckIfSargable(field, queryVal, optionsVal, customFields)

	return rv.count, rv.indexedCount, exact, rv.queryFields, rv.err
}

func (i *FTSIndex) buildQueryAndCheckIfSargable(field string,
	query, options value.Value, customFields interface{}) *sargableRV {
	var qBytes []byte
	var err error

	queryFields, ok := customFields.([]util.SearchField)

	if !ok {
		queryFields, qBytes, err = util.FetchQueryFields(field, query)
		if err != nil {
			return &sargableRV{
				err: util.N1QLError(err, ""),
			}
		}
	}

	var indexOptionAvailable bool
	if options != nil {
		_, indexOptionAvailable = options.Field("index")
	}
	if !indexOptionAvailable {
		// if index field isn't provided within the options section,
		// proceed to check for sargability only if all fields within the
		// query have a common analyzer, and if in case no fields were
		// specified only if the default analyzer is set to standard.
		if len(queryFields) == 0 && i.defaultAnalyzer != "standard" {
			// in sufficient information, not sargable
			return &sargableRV{}
		}

		if len(queryFields) > 0 {
			commonAnalyzer := queryFields[0].Analyzer
			for k := 1; k < len(queryFields); k++ {
				if queryFields[k].Analyzer != commonAnalyzer {
					// not sargable, because query is not verify-able
					return &sargableRV{}
				}
			}
		}
	} else {
		indexVal, _ := options.Field("index")
		if indexVal.Type() == value.OBJECT {
			// if in case this value were an object, it is expected to be
			// a mapping, check if this mapping is compatible with the
			// current index's mapping.
			im, err := util.ConvertValObjectToIndexMapping(indexVal)
			if err != nil {
				return &sargableRV{
					err: util.N1QLError(err, ""),
				}
			}

			searchFields, dynamicMapping, _ :=
				util.SearchableFieldsForIndexMapping(im)

			searchFieldsCompatible := func() bool {
				for k, expect := range searchFields {
					if got, exists := i.searchFields[k]; !exists || got != expect {
						return false
					}
				}
				return true
			}

			if !dynamicMapping && !searchFieldsCompatible() {
				// not sargable, because query is not verify-able
				return &sargableRV{}
			}
		}
	}

	if i.dynamicMapping {
		// sargable, only if all query fields' analyzers are the same
		// as default analyzer.
		compatibleWithDynamicMapping := true
		for k := range queryFields {
			if queryFields[k].Analyzer != "" &&
				queryFields[k].Analyzer != i.defaultAnalyzer {
				compatibleWithDynamicMapping = false
				break
			}
		}
		if compatibleWithDynamicMapping {
			return &sargableRV{
				count:        len(queryFields),
				indexedCount: math.MaxInt64,
				queryFields:  queryFields,
				queryBytes:   qBytes,
			}
		}
	}

	for _, f := range queryFields {
		if f.Analyzer == "" {
			// set analyzer to defaultAnalyzer for those query fields, that
			// don't have an explicit analyzer set already.
			f.Analyzer = i.defaultAnalyzer
		}
		dynamic, exists := i.searchFields[f]
		if exists && dynamic {
			// if searched field contains nested fields, then this field is not
			// searchable, and the query not sargable.
			return &sargableRV{
				queryFields: queryFields,
				queryBytes:  qBytes,
			}
		}

		if !exists {
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
				return &sargableRV{
					queryFields: queryFields,
					queryBytes:  qBytes,
				}
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
						matched = true
						break
					}
				}

				entry += "." + fieldSplitAtDot[k]
			}

			if !matched {
				// not sargable
				return &sargableRV{
					queryFields: queryFields,
					queryBytes:  qBytes,
				}
			}
		}
	}

	// sargable
	return &sargableRV{
		count:        len(queryFields),
		indexedCount: int64(len(i.searchFields)),
		queryFields:  queryFields,
		queryBytes:   qBytes,
	}
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Pageable(order []string, offset, limit int64) bool {
	// Possibilities for order:
	// "score DESC", "score ASC", "score" (defaults to "score ASC")
	if len(order) != 1 {
		// accepts only one of the above possibilities, for now
		return false
	}

	if _, exists := i.optionsForOrdering[order[0]]; !exists {
		return false
	}

	bleveMaxResultWindow, err := i.fetchBleveMaxResultWindow()
	if err != nil {
		return false
	}

	if offset+limit > int64(bleveMaxResultWindow) {
		return false
	}

	return true
}

func (i *FTSIndex) fetchBleveMaxResultWindow() (int, error) {
	ftsEndpoints := i.indexer.agent.FtsEps()
	if len(ftsEndpoints) == 0 {
		return 0, fmt.Errorf("no fts endpoints available")
	}

	now := time.Now().UnixNano()
	cbauthURL, err := cbgt.CBAuthURL(
		ftsEndpoints[now%int64(len(ftsEndpoints))] + "/api/manager")
	if err != nil {
		return 0, err
	}

	httpClient := i.indexer.agent.HttpClient()
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
