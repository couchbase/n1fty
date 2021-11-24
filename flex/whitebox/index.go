//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package whitebox

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/blevesearch/bleve/v2/mapping"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/timestamp"

	"github.com/couchbase/n1fty/flex"
	"github.com/couchbase/n1fty/util"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

type LastSargableFlex struct {
	nodeAlias string
	bindings  expression.Bindings
	where     expression.Expression

	identifiers    flex.Identifiers
	fieldTracks    flex.FieldTracks
	needsFiltering bool

	flexBuild  *flex.FlexBuild
	bleveQuery map[string]interface{}
}

type Index struct {
	SourceName string

	Parent  *Indexer
	IdStr   string
	NameStr string

	IndexMapping    *mapping.IndexMappingImpl
	CondFlexIndexes flex.CondFlexIndexes

	lastSargableFlexOk  *LastSargableFlex // For testing.
	lastSargableFlexErr error
}

func (i *Index) KeyspaceId() string {
	return i.Parent.KeyspaceId()
}

func (i *Index) BucketId() string {
	return ""
}

func (i *Index) ScopeId() string {
	return ""
}

func (i *Index) Id() string {
	return i.IdStr
}

func (i *Index) Name() string {
	return i.NameStr
}

func (i *Index) Type() datastore.IndexType {
	return i.Parent.Name()
}

func (i *Index) Indexer() datastore.Indexer {
	return i.Parent
}

func (i *Index) SeekKey() expression.Expressions {
	return nil // not supported
}

func (i *Index) RangeKey() expression.Expressions {
	return nil // not supported
}

func (i *Index) Condition() expression.Expression {
	return nil // not supported
}

func (i *Index) IsPrimary() bool {
	return false
}

func (i *Index) State() (datastore.IndexState, string, errors.Error) {
	return datastore.ONLINE, "", nil
}

func (i *Index) Statistics(requestID string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, util.N1QLError(nil, "not supported")
}

func (i *Index) Drop(requestID string) errors.Error {
	return util.N1QLError(nil, "not supported")
}

func (i *Index) Scan(requestID string, span *datastore.Span, distinct bool, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(util.N1QLError(nil, "not supported"))
}

func (i *Index) Sargable(field string, query, options expression.Expression, opaque interface{}) (
	int, int64, bool, interface{}, errors.Error) {
	fmt.Printf("i.Sargable, %s, field: %s, query: %v, options: %v\n",
		i.IdStr, field, query, options)

	return 0, 0, false, opaque, nil
}

// Search performs a search/scan over this index, with provided SearchInfo settings
func (i *Index) Search(requestID string, searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	if conn == nil {
		return
	}

	connError := func(e errors.Error) {
		fmt.Printf("  connError: %v\n", e)
		conn.Error(e)
	}

	idx, err := bleve.NewMemOnly(i.IndexMapping)
	if err != nil {
		connError(util.N1QLError(err, "bleve.NewMemOnly"))
		return
	}

	defer idx.Close()

	path := i.Parent.Keyspace.NamespaceId() + "/" +
		i.Parent.Keyspace.Id()

	entries, err1 := ioutil.ReadDir(path)
	if err1 != nil {
		connError(util.N1QLError(err1, "ioutil.ReadDir"))
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		bytes, err2 := ioutil.ReadFile(path + "/" + entry.Name())
		if err2 != nil {
			connError(util.N1QLError(err2, "ioutil.ReadFile"))
			return
		}

		var doc map[string]interface{}

		err = json.Unmarshal(bytes, &doc)
		if err != nil {
			connError(util.N1QLError(err, "json.Unmarshal doc"))
			return
		}

		base := filepath.Base(entry.Name())
		id := base[0 : len(base)-len(filepath.Ext(base))]

		err = idx.Index(id, doc)
		if err != nil {
			connError(util.N1QLError(err, "bleve Insert doc"))
			return
		}
	}

	qBytes, err3 := searchInfo.Query.MarshalJSON()
	if err3 != nil {
		connError(util.N1QLError(err3, "searchInfo.Query.MarshalJSON"))
		return
	}

	q, err4 := query.ParseQuery(qBytes)
	if err4 != nil {
		connError(util.N1QLError(err4, "query.ParseQuery"))
		return
	}

	sr := bleve.NewSearchRequest(q)

	res, err5 := idx.Search(sr)
	if err5 != nil {
		connError(util.N1QLError(err5, "idx.Search"))
		return
	}

	sender := conn.Sender()

	defer sender.Close()

	for _, hit := range res.Hits {
		if !sender.SendEntry(&datastore.IndexEntry{PrimaryKey: hit.ID}) {
			return
		}
	}
}

func (i *Index) Pageable(order []string, offset, limit int64,
	query, options expression.Expression) bool {
	fmt.Printf("i.Pageable, %s, order: %+v, offset: %v, limit: %v\n",
		i.IdStr, order, offset, limit)

	return false
}

// -----------------------------------------------------------------

func (i *Index) SargableFlex(nodeAlias string, bindings expression.Bindings,
	where expression.Expression, opaque interface{}) (
	sargLength int, exact bool, searchQuery, searchOptions map[string]interface{},
	opaqueOut interface{}, err errors.Error) {
	fmt.Printf("i.SargableFlex, nodeAlias: %s\n", nodeAlias)
	fmt.Printf("  where: %v\n", where)
	for _, b := range bindings {
		fmt.Printf("  binding: %+v, expr: %+v\n", b, b.Expression())
	}

	identifiers := flex.Identifiers{flex.Identifier{Name: nodeAlias}}

	var ok bool
	identifiers, ok = identifiers.Push(bindings, -1)
	if !ok {
		return 0, false, nil, nil, opaque, nil
	}

	fmt.Printf("  identifiers: %+v\n", identifiers)

	fieldTracks, needsFiltering, flexBuild, err0 := i.CondFlexIndexes.Sargable(
		identifiers, where, nil)

	i.lastSargableFlexErr = err0

	if err0 != nil {
		fmt.Printf("   CondFlexIndexes.Sargable err0: %v\n", err0)
		return 0, false, nil, nil, opaque, util.N1QLError(err0, "")
	}

	if len(fieldTracks) <= 0 {
		fmt.Printf("   CondFlexIndexes.Sargable len(fieldTracks) <= 0\n")
		j, _ := json.Marshal(i.CondFlexIndexes)
		fmt.Printf("    CondFlexIndexes: %s\n", j)
		return 0, false, nil, nil, opaque, nil
	}

	bleveQuery, err1 := flex.FlexBuildToBleveQuery(flexBuild, nil)
	if err1 != nil {
		fmt.Printf("   flex.FlexBuildToBleveQuery err1: %v\n", err1)
		return 0, false, nil, nil, opaque, util.N1QLError(err1, "")
	}

	bleveQueryJ, _ := json.Marshal(bleveQuery)

	fmt.Printf("  bleveQuery: %s\n", bleveQueryJ)
	fmt.Printf("  fieldTracks: %v, needsFiltering: %v\n", fieldTracks, needsFiltering)

	i.lastSargableFlexOk = &LastSargableFlex{
		nodeAlias: nodeAlias,
		bindings:  bindings,
		where:     where,

		identifiers: identifiers,

		fieldTracks:    fieldTracks,
		needsFiltering: needsFiltering,
		flexBuild:      flexBuild,

		bleveQuery: bleveQuery,
	}

	return len(fieldTracks), !needsFiltering, bleveQuery, nil, opaque, nil
}
