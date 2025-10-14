//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/v2/mapping"
	bq "github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/flex"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

const doneRequest = int64(1)

// FTSIndex implements datastore.FTSIndex interface
type FTSIndex struct {
	indexer  *FTSIndexer
	indexDef *cbgt.IndexDef

	// map of SearchFields to dynamic-ness
	searchableFields map[util.SearchField]bool
	// number of indexed fields
	indexedCount int64

	condExpr expression.Expression

	// map of dynamic mappings to their default analyzers
	dynamicMappings map[string]string

	allFieldSearchable    bool // true if _all field contains some content for collection
	defaultAnalyzer       string
	defaultDateTimeParser string
	multipleTypeStrs      bool
	multiCollectionIndex  bool

	// flex indexes supported
	condFlexIndexes flex.CondFlexIndexes
}

// -----------------------------------------------------------------------------

func newFTSIndex(indexer *FTSIndexer, indexDef *cbgt.IndexDef,
	pip util.ProcessedIndexParams) (rv *FTSIndex, err error) {
	var condExpr expression.Expression
	if len(pip.CondExpr) > 0 {
		condExpr, err = parser.Parse(pip.CondExpr)
		if err != nil {
			return nil, err
		}
	}

	index := &FTSIndex{
		indexer:               indexer,
		indexDef:              indexDef,
		searchableFields:      pip.SearchFields,
		indexedCount:          pip.IndexedCount,
		condExpr:              condExpr,
		dynamicMappings:       pip.DynamicMappings,
		allFieldSearchable:    pip.AllFieldSearchable,
		defaultAnalyzer:       pip.DefaultAnalyzer,
		defaultDateTimeParser: pip.DefaultDateTimeParser,
		multipleTypeStrs:      len(pip.TypeMappings) > 1,
		multiCollectionIndex:  pip.MultiCollectionIndex,
	}

	condFlexIndexes, err := flex.BleveToCondFlexIndexes(
		index.Name(), index.Id(),
		pip.IndexMapping, pip.DocConfig, pip.Scope, pip.Collection)
	if err == nil && len(condFlexIndexes) > 0 {
		index.condFlexIndexes = condFlexIndexes
	}

	return index, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) KeyspaceId() string {
	return i.indexer.KeyspaceId()
}

func (i *FTSIndex) BucketId() string {
	return i.indexer.BucketId()
}

func (i *FTSIndex) ScopeId() string {
	return i.indexer.ScopeId()
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
	return i.condExpr // Non-nil, for example, when 'type IN ["beer"]'.
}

func (i *FTSIndex) IsPrimary() bool {
	return false
}

func (i *FTSIndex) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (i *FTSIndex) Statistics(requestID string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, util.N1QLError(nil, "Statistics not supported yet")
}

func (i *FTSIndex) Drop(requestID string) errors.Error {
	return util.N1QLError(nil, "Drop not supported")
}

func (i *FTSIndex) Scan(requestID string, span *datastore.Span, distinct bool,
	limit int64, cons datastore.ScanConsistency,
	vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(util.N1QLError(nil, "Scan not supported"))
	return
}

// Search performs a search/scan over this index, with provided SearchInfo settings
func (i *FTSIndex) Search(requestID string, searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	if util.Debug > 0 {
		logging.Infof("n1fty: Search, index: %s, requestID: %s, searchInfo: %+v,"+
			" cons: %v, vector: %v\n",
			i.indexDef.Name, requestID, searchInfo, cons, vector)
	}

	if conn == nil {
		return
	}

	sender := conn.Sender()

	if sender == nil {
		conn.Error(util.N1QLError(nil, "conn's Sender not defined"))
		return
	}

	if searchInfo == nil || searchInfo.Query == nil {
		conn.Error(util.N1QLError(nil, "no search parameters provided"))
		sender.Close()
		return
	}

	if cons == datastore.SCAN_PLUS {
		conn.Error(util.N1QLError(nil, "scan_plus consistency not supported"))
		sender.Close()
		return
	}

	field := ""
	if searchInfo.Field != nil {
		if fieldStr, ok := searchInfo.Field.Actual().(string); ok {
			field = fieldStr
		} else {
			conn.Error(util.N1QLError(nil, "field provided must be of type:string"))
			sender.Close()
			return
		}
	}

	// this sargable(...) check is to ensure that the query is indeed "sargable"
	// at search time, as when the Sargable(..) API is invoked during the
	// prepare time, the query/options may not have been available.
	sargRV := i.buildQueryAndCheckIfSargable(
		field, searchInfo.Query, searchInfo.Options, nil)
	if sargRV.err != nil || sargRV.count == 0 {
		conn.Error(util.N1QLError(sargRV.err, "not sargable"))
		sender.Close()
		return
	}

	searchRequest := sargRV.searchRequest
	if i.indexer != nil && i.indexer.collectionAware {
		// Decorate the search request while addressing a collection aware index
		// with the collection filter.
		searchRequest = util.DecorateSearchRequest(searchRequest, i.indexer.collection)
	}

	starttm := time.Now()

	var waitGroup sync.WaitGroup
	var backfillSync int64
	var rh *responseHandler
	var err error

	var ctx context.Context
	var cancel context.CancelFunc

	timeoutMS := conn.GetReqDeadline().Sub(time.Now()) * time.Millisecond
	if timeoutMS > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeoutMS)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

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

	searchReq, err := util.BuildProtoSearchRequest(searchRequest, searchInfo,
		vector, cons, i.indexDef.Name)
	if err != nil {
		conn.Error(util.N1QLError(err, "search request parse err"))
		return
	}

	if sargRV.timeoutMS <= 0 {
		// If timeout specified in SearchRequest, apply it to the
		// gRPC search request, otherwise default to 2 minutes
		sargRV.timeoutMS = 120000 // defaults to 2min
	}

	queryCtlParams := &pb.QueryCtlParams{
		Ctl: &pb.QueryCtl{
			Timeout: sargRV.timeoutMS,
		},
	}

	searchReq.QueryCtlParams, _ = json.Marshal(queryCtlParams)

	ftsClient := i.indexer.getClient()
	if ftsClient == nil {
		conn.Error(util.N1QLError(nil, "client unavailable, try refreshing"))
		return
	}

	client, connPool, clientConn := ftsClient.getGrpcClient()
	if client == nil {
		//todo: Better error message
		conn.Error(util.N1QLError(nil, "gRPC client unavailable, try refreshing"))
		return
	}
	defer func() {
		connPool.yield(clientConn)
	}()

	stream, err := client.Search(ctx, searchReq)
	if err != nil || stream == nil {
		conn.Error(util.N1QLError(err, "search failed"))
		return
	}

	rh = newResponseHandler(i, requestID, sargRV.searchRequest)

	rh.handleResponse(conn, &waitGroup, &backfillSync, stream)

	atomic.AddInt64(&i.indexer.stats.TotalSearch, 1)
	atomic.AddInt64(&i.indexer.stats.TotalSearchDuration, int64(time.Since(starttm)))
}

// -----------------------------------------------------------------------------

type sargableRV struct {
	count         int
	indexedCount  int64
	exact         bool
	containsKNN   bool
	opaque        map[string]interface{}
	searchRequest *cbft.SearchRequest
	timeoutMS     int64
	err           errors.Error
}

// Sargable checks if the provided request is applicable for the index.
//
// Return parameters:
//   - sargable_count: This is the number of fields whose names along with
//     analyzers from the built query matched with that of
//     the index definition, for now all of query fields or 0.
//   - indexed_count:  This is the total number of indexed fields within the
//     the FTS index.
//   - exact:          True if the query would produce no false positives
//     using this FTS index.
//     (place holder for when partial sargability is supported)
//   - contains_knn:   True if SEARCH function contains KNN elements
//   - opaque:         The map of certain contextual data that can be re-used
//     as query iterates through several FTSIndexes.
//     (in-out parameter)
//
// Contents of opaque:
//   - an entry for query field-type-analyzers
//   - an entry for searchable fields obtained from index option
//   - an entry for the search request generated from the query & field.
//
// The caller will have to make the decision on which index to choose based
// on the sargable_count (higher the better), indexed_count (lower the better),
// and exact (if true) returned.
func (i *FTSIndex) Sargable(field string, query,
	options expression.Expression, opaque interface{}) (
	int, int64, bool, bool, interface{}, errors.Error) {
	var queryVal, optionsVal value.Value
	if query != nil {
		queryVal = query.Value()
	}
	if options != nil {
		optionsVal = options.Value()
	}

	if i.multipleTypeStrs {
		// As this index includes multiple type mappings, this index will
		// not be supported for n1ql's SEARCH(..) functions to avoid the
		// possibility of false positives, as FTS supports indexing same
		// field names across type mappings.
		return 0, 0, false, false, nil, nil
	}

	var queryFields map[util.SearchField]struct{}
	if opq, ok := opaque.(map[string]interface{}); ok {
		if _, exists := opq["query_fields"]; exists {
			queryFields, _ = opq["query_fields"].(map[util.SearchField]struct{})
		}
	}

	if queryVal == nil && len(queryFields) == 0 {
		// this index will be sargable for the unavailable query if
		// it has a default dynamic mapping with the _all field searchable;
		// if _all field isn't searchable or index is multi-collection,
		// this index isn't sargable for an unavailable query.
		if len(i.dynamicMappings) > 0 && i.allFieldSearchable && !i.multiCollectionIndex {
			return int(math.MaxInt64), math.MaxInt64, false, false, opaque, nil
		}

		// if the index isn't default dynamic, check if the query expression
		// provided is an object construct, to retrieve any available "field" information;
		// use available field information to determine the sargability of
		// this index for the field(s), ignoring the analyzer, type etc. for
		// now; sargability is tested for again during search time when the
		// query becomes available.
		queryFields = map[util.SearchField]struct{}{}
		var containsKnn bool

		var fetchFields func(expression.Expression)
		fetchFields = func(arg expression.Expression) {
			if oc, ok := arg.(*expression.ObjectConstruct); ok {
				for name, val := range oc.Mapping() {
					if nVal := name.Value(); nVal != nil &&
						nVal.Type() == value.STRING && nVal.Actual().(string) == "knn" {
						if val.Type() == value.ARRAY {
							containsKnn = true
							// iterate over "knn" array to extract pre-filter requests
							if knnArr, ok := val.(*expression.ArrayConstruct); ok {
								// iterate over entries to look for "filter" field
								for _, v := range knnArr.Operands() {
									if vOC, ok := v.(*expression.ObjectConstruct); ok {
										// iterate over entries to look for "filter" field
										for n1, v1 := range vOC.Mapping() {
											if n1Val := n1.Value(); n1Val != nil &&
												n1Val.Type() == value.STRING &&
												n1Val.Actual().(string) == "filter" {
												fetchFields(v1)
											}
										}
									}
								}
							}
						}
					} else if nVal != nil && nVal.Type() == value.STRING && nVal.Actual().(string) == "field" {
						if val.Value() != nil && val.Value().Type() == value.STRING {
							queryFields[util.SearchField{
								Name: val.Value().Actual().(string),
							}] = struct{}{}
						}
					} else {
						fetchFields(val)
					}
				}
			} else if ac, ok := arg.(*expression.ArrayConstruct); ok {
				for _, entry := range ac.Operands() {
					fetchFields(entry)
				}
			}
		}

		fetchFields(query)

		if len(queryFields) > 0 {
			opq, ok := opaque.(map[string]interface{})
			if !ok {
				opq = make(map[string]interface{})
			}
			opq["query_fields"] = queryFields
			opq["contains_knn"] = containsKnn
			opaque = opq
		}
	}

	rv := i.buildQueryAndCheckIfSargable(field, queryVal, optionsVal, opaque)

	if util.Debug > 0 {
		logging.Infof("n1fty: Sargable, index: %s, field: %s, query: %v,"+
			" options: %v, rv: %+v",
			i.indexDef.Name, field, query, options, rv)
	}

	return rv.count, rv.indexedCount, rv.exact, rv.containsKNN, rv.opaque, rv.err
}

func (i *FTSIndex) buildQueryAndCheckIfSargable(field string,
	query, options value.Value, opaque interface{}) *sargableRV {
	rv := &sargableRV{}

	var ok bool
	rv.opaque, ok = opaque.(map[string]interface{})
	if !ok {
		rv.opaque = make(map[string]interface{})
	}

	var err error
	var queryFields map[util.SearchField]struct{}
	var sr *cbft.SearchRequest
	var ctlTimeout int64
	var containsKNN, needsFiltering bool

	if queryFieldsInterface, exists := rv.opaque["query_fields"]; !exists {
		// if opaque didn't carry a "query" entry, go ahead and
		// process the field+query provided to retrieve queryFields.
		queryFields, sr, ctlTimeout, containsKNN, needsFiltering, err =
			util.ParseQueryToSearchRequest(field, query)
		if err != nil {
			rv.err = util.N1QLError(err, "failed to parse query to search request")
			return rv
		}

		// update opaqueMap with query, search_request
		rv.opaque["query_fields"] = queryFields
		rv.opaque["search_request"] = sr
		rv.opaque["ctl_timeout"] = ctlTimeout
		rv.opaque["contains_knn"] = containsKNN
		rv.opaque["needs_filtering"] = needsFiltering
	} else {
		queryFields, _ = queryFieldsInterface.(map[util.SearchField]struct{})

		// if an entry for "query" exists, we can assume that an entry for
		// "search_request" also exists.
		srInterface, _ := rv.opaque["search_request"]
		sr, _ = srInterface.(*cbft.SearchRequest)
		ctlTimeout, _ = rv.opaque["ctl_timeout"].(int64)
		containsKNN, _ = rv.opaque["contains_knn"].(bool)
		needsFiltering, _ = rv.opaque["needs_filtering"].(bool)
	}

	rv.searchRequest = sr
	rv.timeoutMS = ctlTimeout
	rv.exact = !needsFiltering
	rv.containsKNN = containsKNN

	if options != nil {
		// check if an "index" entry exists and if it matches
		indexVal, exists := options.Field("index")
		if exists {
			if indexVal.Type() == value.OBJECT {
				var im *mapping.IndexMappingImpl
				// check if opaque carries an "index" entry
				if imInterface, exists := rv.opaque["index_mapping"]; !exists {
					// if in case this value were an object, it is expected to be
					// a mapping, check if this mapping is compatible with the
					// current index's mapping.
					im, err = util.ConvertValObjectToIndexMapping(indexVal)
					if err != nil {
						rv.err = util.N1QLError(err, "index mapping option isn't valid")
						return rv
					}

					// update opaqueMap
					rv.opaque["index_mapping"] = im
				} else {
					im, _ = imInterface.(*mapping.IndexMappingImpl)
				}

				searchableFields, _, _, dynamicMappings, _,
					defaultAnalyzer, defaultDateTimeParser, _ := util.ProcessIndexMapping(im, "", "")

				if len(dynamicMappings) == 0 && len(i.dynamicMappings) == 0 {
					// no dynamic mappings
					for k, expect := range searchableFields {
						if got, exists := i.searchableFields[k]; !exists || got != expect {
							return rv
						}
					}

					if (defaultAnalyzer != "" && defaultAnalyzer != i.defaultAnalyzer) ||
						(defaultDateTimeParser != "" &&
							defaultDateTimeParser != i.defaultDateTimeParser) {
						return rv
					}
				}
			} else if indexVal.Type() == value.STRING {
				// if an index name has been provided, check if the current index
				// shares the same name; if not this index is not sargable, also
				// check for indexUUID if available.
				if i.Name() != indexVal.Actual().(string) {
					// not sargable
					return rv
				}
			}
		}

		// check if an "indexUUID" entry exists and if it matches
		indexUUIDVal, indexUUIDAvailable := options.Field("indexUUID")
		if indexUUIDAvailable && indexUUIDVal.Type() == value.STRING {
			if i.Id() != indexUUIDVal.Actual().(string) {
				// not sargable
				return rv
			}
		}
	}

	for _, defaultAnalyzer := range i.dynamicMappings {
		// sargable, only if all query fields' analyzers are the same
		// as the default analyzer for one of the available dynamic
		// mapping.
		compatibleWithDynamicMapping := true
		for f := range queryFields {
			if (f.Analyzer != "" && f.Analyzer != defaultAnalyzer) ||
				(f.Type == "vector" && f.Dims > 0) {
				compatibleWithDynamicMapping = false
				break
			}
		}
		if compatibleWithDynamicMapping {
			var count int
			for qf, _ := range queryFields {
				if len(qf.Name) == 0 {
					// if even a single sub-query doesn't have its field set,
					// reset count to 0, and overwrite sargable count to the
					// number of fields indexed.
					count = 0
					break
				}
				count++
			}
			rv.count = count
			if rv.count == 0 {
				// if field(s) not provided or unavailable within query,
				// search is applicable on all indexed fields.
				rv.count = int(i.indexedCount)
			}
			rv.indexedCount = i.indexedCount
			return rv
		}
	}

	isParentFieldSearchable := func(field util.SearchField) bool {
		// check if a prefix of this field name is searchable.
		// - (prefix being delimited by ".")
		// e.g.: potential candidates for "reviews.review.content.author" are:
		// - reviews
		// - reviews.review
		// - reviews.review.content
		// .. only if any of the above mappings are dynamic.
		fieldSplitAtDot := strings.Split(field.Name, ".")
		if len(fieldSplitAtDot) <= 1 {
			// not sargable
			return false
		}

		var matched bool
		entry := fieldSplitAtDot[0]
		for k := 1; k < len(fieldSplitAtDot); k++ {
			searchField := util.SearchField{
				Name:     entry,
				Analyzer: field.Analyzer,
			}
			if dynamic, exists := i.searchableFields[searchField]; exists {
				if dynamic {
					matched = true
					break
				}
			}

			entry += "." + fieldSplitAtDot[k]
		}

		if !matched {
			// not sargable
			return false
		}

		return true
	}

	var count int
	for f := range queryFields {
		if f.Name == "" {
			// field name not provided/available
			// check if index supports _all field, if not, this query is not sargable;
			// if _all field is searchable but index is multi-collection, not sargable
			if !i.allFieldSearchable || i.multiCollectionIndex {
				return rv
			}

			// move on to next query field
			continue
		}

		count++
		if f.Type == "" {
			// type isn't available, likely because query value wasn't available;
			// check field name against all possible types
			for _, typ := range []string{
				"boolean", "number", "text", "datetime", "geopoint", "geoshape", "IP", "vector",
			} {
				f.Type = typ
				if typ == "text" {
					f.Analyzer = ""
				} else if typ == "datetime" {
					f.DateFormat = i.defaultDateTimeParser
				}

				dynamic, exists := i.searchableFields[f]
				if exists && dynamic {
					// not sargable
				} else if exists {
					break
				} else if !exists {
					if isParentFieldSearchable(f) {
						break
					}
					// else not sargable
				}
				f.Type = ""
				f.Analyzer = ""
				f.DateFormat = ""
			}

			if f.Type == "" {
				// not sargable
				return rv
			}

		} else {
			if f.Type == "text" {
				// Overwrite the analyzer in the query "", so we can
				// allow any kind of analytic query regardless of the index definition.
				// (MB-53231, MB-68274)
				f.Analyzer = ""
				f.DateFormat = ""
			} else if f.Type == "datetime" && f.DateFormat == "" {
				f.Analyzer = ""
				f.DateFormat = i.defaultDateTimeParser
			}

			dynamic, exists := i.searchableFields[f]
			if exists && dynamic {
				// if searched field contains nested fields, then this field is not
				// searchable, and the query not sargable.
				return rv
			}

			if !exists {
				if !isParentFieldSearchable(f) {
					// not sargable
					return rv
				}
			}
		}
	}

	rv.count = count
	if rv.count == 0 {
		if sr != nil {
			// if field(s) not available within the query, check if it's
			// a match all query
			if que, err := bq.ParseQuery(sr.Q); err == nil {
				if _, ok := que.(*bq.MatchAllQuery); ok {
					rv.count = 1
					return rv
				}
			}
		}

		// if field(s) not provided or unavailable within query,
		// index is not sargable if it does not support _all field;
		// if _all is availbale but index is multi-collection, not sargable
		if !i.allFieldSearchable || i.multiCollectionIndex {
			return rv
		}

		// search is applicable on all indexed fields.
		rv.count = int(i.indexedCount)
	}

	// sargable
	rv.indexedCount = i.indexedCount
	return rv
}

// -----------------------------------------------------------------------------

// Pageable returns `true` when it can deliver sorted paged results
// for the requested parameters, and the options are consistent
// across the order[] and the query parameters.
func (i *FTSIndex) Pageable(order []string, offset, limit int64, query,
	options expression.Expression) bool {
	rv := i.pageable(order, offset, limit, query, options)

	if util.Debug > 0 {
		logging.Infof("n1fty: Pageable, index: %s, order: %v,"+
			" offset: %v, limit: %v, query: %v, options: %v, rv: %t",
			i.indexDef.Name, order, offset, limit, query, options, rv)
	}

	return rv
}

func (i *FTSIndex) pageable(order []string, offset, limit int64, query,
	options expression.Expression) bool {
	var queryVal value.Value
	if query != nil {
		queryVal = query.Value()
	}

	// if query contains a searchRequest with some valid pagination
	// info(From, Size or Sort details) then returns false.
	if queryVal != nil {
		if qf, ok := queryVal.Field("query"); ok && qf.Type() == value.OBJECT {
			if util.CheckForPagination(queryVal) {
				// User provided pagination details that could possibly
				// conflict with higher offset/limit settings
				return false
			}
		}
	}

	return uint64(offset)+uint64(limit) <= util.GetBleveMaxResultWindow()
}

// -----------------------------------------------------------------------------

// SargableFlex transforms N1QL predicate to Search() function request
func (i *FTSIndex) SargableFlex(requestId string,
	req *datastore.FTSFlexRequest) (
	*datastore.FTSFlexResponse, errors.Error) {
	if len(i.condFlexIndexes) == 0 {
		return nil, nil
	}

	identifiers := flex.Identifiers{flex.Identifier{Name: req.Keyspace}}

	var ok bool
	identifiers, ok = identifiers.Push(req.Bindings, -1)
	if !ok {
		return nil, util.N1QLError(nil, "SargableFlex bindings")
	}

	fieldTracks, needsFiltering, flexBuild, sortDetails, err := i.condFlexIndexes.Sargable(
		identifiers, req.Pred, req.Keyspace, nil)
	if err != nil {
		return nil, util.N1QLError(err, "SargableFlex Sargable")
	}

	if len(fieldTracks) == 0 {
		return nil, nil
	}

	bleveQuery, err := flex.FlexBuildToBleveQuery(flexBuild, nil)
	if err != nil {
		return nil, util.N1QLError(err, "SargableFlex Sargable")
	}

	if bleveQuery == nil {
		return nil, nil
	}

	knnQuery, knnOperator, supported, err := flex.FlexBuildToKNNQuery(flexBuild, nil)
	if err != nil {
		return nil, util.N1QLError(err, "SargableFlex Sargable")
	}
	if !supported {
		return nil, nil
	}

	if !needsFiltering {
		// Overwrite needsFiltering (only if unset) to true if we need to
		// for the query (involves match_all, negate).
		needsFiltering, err = util.FlexQueryNeedsFiltering(bleveQuery)
		if err != nil {
			return nil, nil
		}
	}

	searchRequest := map[string]interface{}{
		"query": bleveQuery,
		"score": "none",
	}

	var containsKNN bool

	if len(knnQuery) > 0 {
		searchRequest["knn"] = knnQuery
		if len(knnOperator) > 0 {
			searchRequest["knn_operator"] = knnOperator
		}

		containsKNN = true

		// Make search requests with kNN non-covered for now, so the
		// query engine will be able to remove false positives.
		// False positives can occur because of the search engine's
		// ability to _only_ OR the kNN part with the query part.
		// This limitation can be lifted when the operator (knn <> query)
		// can be pushed down to the search engine.
		needsFiltering = true
	}

	searchOptions := map[string]interface{}{
		"index": i.Name(),
	}

	stringer := expression.NewStringer()

	res := &datastore.FTSFlexResponse{}
	res.SearchOptions = stringer.Visit(expression.NewConstant(value.NewValue(searchOptions)))
	res.StaticSargKeys = make(map[string]expression.Expression, 8)

	for s := range fieldTracks {
		e, _ := parser.Parse(string(s))
		res.StaticSargKeys[string(s)] = e
	}

	res.NumIndexedKeys = uint32(i.indexedCount)
	if !needsFiltering && !i.multipleTypeStrs {
		res.RespFlags |= datastore.FTS_FLEXINDEX_EXACT

		if req.CheckPageable {
			// Check for pageability of the index, only if EXACT
			// Set Offset, Limit settings only if:
			//     - they fall within the BleveMaxResultWindow
			//     - ORDER BY is requested over fields that are indexed
			if uint64(req.Offset)+uint64(req.Limit) <= util.GetBleveMaxResultWindow() {
				sortable := true
				var sortOrder []string
				var sortOrderVals []value.Value

				metaIdExpr := expression.NewField(
					expression.NewMeta(expression.NewIdentifier(req.Keyspace)),
					expression.NewFieldName("id", false))
				for i := range req.Order {
					// Note: Order by score is NOT supported for FLEX queries

					var sortStr string

					if req.Order[i].Expr.EquivalentTo(metaIdExpr) {
						// Check if it's the meta().id field and if that's
						// the case the sortStr is the "_id" field indexed by FTS
						sortStr = "_id"
					} else if sortDetails != nil {
						alias, orderBy, err := expression.PathString(req.Order[i].Expr)
						if err != nil || alias != req.Keyspace {
							sortable = false
							break
						}

						// Drop any backticks
						orderBy = util.CleanseField(orderBy)

						_, exists := sortDetails.Fields[orderBy]
						if !exists && !sortDetails.Dynamic {
							// Field is NOT indexed and index is NOT dynamic
							sortable = false
							break
						}

						sortStr = orderBy
					}

					if req.Order[i].Descending {
						sortStr = "-" + sortStr
					}

					sortOrder = append(sortOrder, sortStr)
					sortOrderVals = append(sortOrderVals, value.NewValue(sortStr))
				}

				if sortable {
					// Update searchRequest settings
					searchRequest["size"] = req.Limit
					searchRequest["from"] = req.Offset
					if len(sortOrder) > 0 {
						searchRequest["sort"] = sortOrderVals
					}

					// FTS index can handle LIMIT, OFFSET, ORDER settings
					res.RespFlags |= datastore.FTS_FLEXINDEX_LIMIT
					res.RespFlags |= datastore.FTS_FLEXINDEX_OFFSET
					res.RespFlags |= datastore.FTS_FLEXINDEX_ORDER

					res.SearchOrders = sortOrder
				}
			}
		}
	}

	res.SearchQuery = stringer.Visit(expression.NewConstant(value.NewValue(searchRequest)))
	res.ContainsKNN = containsKNN

	return res, nil
}
