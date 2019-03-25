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

	"math"

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
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

const doneRequest = int64(1)

// FTSIndex implements datastore.FTSIndex interface
type FTSIndex struct {
	indexer  *FTSIndexer
	indexDef *cbgt.IndexDef

	// map of SearchFields to dynamic-ness
	searchFields map[util.SearchField]bool

	condExpr expression.Expression

	dynamic bool // true if a top-level dynamic mapping is enabled

	defaultAnalyzer       string
	defaultDateTimeParser string

	// supported options for ordering
	optionsForOrdering map[string]struct{}
}

// -----------------------------------------------------------------------------

func newFTSIndex(indexer *FTSIndexer,
	indexDef *cbgt.IndexDef,
	searchFields map[util.SearchField]bool,
	condExprStr string,
	dynamic bool,
	defaultAnalyzer string,
	defaultDateTimeParser string) (rv *FTSIndex, err error) {
	var condExpr expression.Expression
	if len(condExprStr) > 0 {
		condExpr, err = parser.Parse(condExprStr)
		if err != nil {
			return nil, err
		}
	}

	index := &FTSIndex{
		indexer:               indexer,
		indexDef:              indexDef,
		searchFields:          searchFields,
		condExpr:              condExpr,
		dynamic:               dynamic,
		defaultAnalyzer:       defaultAnalyzer,
		defaultDateTimeParser: defaultDateTimeParser,
		optionsForOrdering:    make(map[string]struct{}),
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
	return i.indexDef.UUID
}

func (i *FTSIndex) Name() string {
	return i.indexDef.Name
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
	return i.condExpr // Non-nil, for example, when 'type="beer"'.
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

	if cons == datastore.SCAN_PLUS {
		conn.Error(util.N1QLError(nil, "scan_plus consistency not supported"))
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
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		atomic.StoreInt64(&backfillSync, doneRequest)
		waitGroup.Wait()
		sender.Close()
		cancel()
		// cleanup the backfill file
		if rh != nil {
			rh.cleanupBackfill()
		}
	}()

	util.ParseSearchInfoToSearchRequest(&sargRV.searchRequest.protoMsg, searchInfo,
		vector, cons, i.indexDef.Name, sargRV.searchRequest.complete)

	client := i.indexer.srvWrapper.getGrpcClient()
	if client == nil {
		conn.Error(util.N1QLError(nil, "client unavailable, try refreshing"))
		return
	}

	stream, err := client.Search(ctx, sargRV.searchRequest.protoMsg)
	if err != nil || stream == nil {
		conn.Error(util.N1QLError(err, "search failed"))
		return
	}

	rh = newResponseHandler(i, requestId, sargRV.searchRequest.protoMsg.Contents)

	rh.handleResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalSearch, 1)
	atomic.AddInt64(&i.indexer.stats.TotalSearchDuration, int64(time.Since(starttm)))
}

// -----------------------------------------------------------------------------

type SearchRequest struct {
	protoMsg *pb.SearchRequest
	complete bool
}

type sargableRV struct {
	count         int
	indexedCount  int64
	queryFields   []util.SearchField
	searchRequest *SearchRequest
	err           errors.Error
}

// Sargable checks if the provided request is applicable for the index.
// Return parameters:
// - sargable_count: This is the number of fields whose names along with
//                   analyzers from the built query matched with that of
//                   the index definition, for now all of query fields or 0.
// - indexed_count:  This is the total number of indexed fields within the
//                   the FTS index.
// - exact:          True if the query would produce no false positives
//                   using this FTS index.
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

	// TODO: this does not seem like the right false-positives check?
	exact := (queryVal != nil) && (options == nil || optionsVal != nil)

	rv := i.buildQueryAndCheckIfSargable(field, queryVal, optionsVal, customFields)

	return rv.count, rv.indexedCount, exact, rv.queryFields, rv.err
}

func (i *FTSIndex) buildQueryAndCheckIfSargable(field string,
	query, options value.Value, opaqueObj interface{}) *sargableRV {
	var err error

	var req *pb.SearchRequest
	var isSearchRequest bool
	queryFields, ok := opaqueObj.([]util.SearchField)
	if !ok {
		queryFields, req, isSearchRequest, err = util.ParseQueryToSearchRequest(field,
			query, opaqueObj)
		if err != nil {
			return &sargableRV{
				err: util.N1QLError(err, ""),
			}
		}
	}

	sr := &SearchRequest{protoMsg: req,
		complete: isSearchRequest,
	}

	if options != nil {
		indexVal, exists := options.Field("index")
		if exists {
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

				searchFields, _, dynamic, _, _ := util.ProcessIndexMapping(im)

				if !dynamic {
					searchFieldsCompatible := true
					for k, expect := range searchFields {
						if got, exists := i.searchFields[k]; !exists || got != expect {
							searchFieldsCompatible = false
							break
						}
					}

					if !searchFieldsCompatible {
						// not sargable, because explicit mapping isn't compatible
						return &sargableRV{}
					}
				}
			}
		}
	}

	if i.dynamic {
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
			sargableCount := len(queryFields)
			if sargableCount == 0 {
				// if field(s) not provided or unavailable within query,
				// search is applicable on all indexed fields.
				sargableCount = math.MaxInt64
			}
			return &sargableRV{
				count:         sargableCount,
				indexedCount:  math.MaxInt64,
				queryFields:   queryFields,
				searchRequest: sr,
			}
		}
	}

	for _, f := range queryFields {
		if f.Type == "text" && f.Analyzer == "" {
			// set analyzer to defaultAnalyzer for those query fields of type:text,
			// that don't have an explicit analyzer set already.
			//
			// TODO: double-check if this mutation is ok, or if we
			// instead need to copy f / copy-on-write.
			f.Analyzer = i.defaultAnalyzer
			f.DateFormat = ""
		} else if f.Type == "datetime" && f.DateFormat == "" {
			f.Analyzer = ""
			f.DateFormat = i.defaultDateTimeParser
		}

		if f.Name == "" {
			// field name not provided/available => sargable on all indexed fields,
			// can skip processing the rest of the fields.
			return &sargableRV{
				count:         len(i.searchFields),
				indexedCount:  int64(len(i.searchFields)),
				queryFields:   queryFields,
				searchRequest: sr,
			}
		}

		dynamic, exists := i.searchFields[f]
		if exists && dynamic {
			// if searched field contains nested fields, then this field is not
			// searchable, and the query not sargable.
			return &sargableRV{
				queryFields:   queryFields,
				searchRequest: sr,
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
					indexedCount:  int64(len(i.searchFields)),
					queryFields:   queryFields,
					searchRequest: sr,
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
					indexedCount:  int64(len(i.searchFields)),
					queryFields:   queryFields,
					searchRequest: sr,
				}
			}
		}
	}

	sargableCount := len(queryFields)
	if sargableCount == 0 {
		// if field(s) not provided or unavailable within query,
		// search is applicable on all indexed fields.
		sargableCount = len(i.searchFields)
	}

	// sargable
	return &sargableRV{
		count:         sargableCount,
		indexedCount:  int64(len(i.searchFields)),
		queryFields:   queryFields,
		searchRequest: sr,
	}
}

// -----------------------------------------------------------------------------

// Pageable returns `true` when it can deliver sorted paged results
// for the requested parameters, and the options are consistent
// across the order[] and the query parameters.
func (i *FTSIndex) Pageable(order []string, offset, limit int64, query,
	options expression.Expression) bool {
	var queryVal value.Value
	if query != nil {
		queryVal = query.Value()
	}

	// if query contains a searchRequest, then returns false
	if qf, ok := queryVal.Field("query"); ok && qf.Type() == value.OBJECT {
		return false
	}

	return offset+limit <= util.GetBleveMaxResultWindow()
}

func isEqual(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
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
	conf := clientConfig.GetConfig()
	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[backfillSpaceDir]; ok {
		return v.(string)
	}

	return getDefaultTmpDir()
}

func getBackfillSpaceLimit() int64 {
	conf := clientConfig.GetConfig()
	if conf == nil {
		return defaultBackfillLimit
	}

	if v, ok := conf[backfillSpaceLimit]; ok {
		return v.(int64)
	}

	return defaultBackfillLimit
}
