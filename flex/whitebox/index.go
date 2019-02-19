//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package whitebox

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/timestamp"

	"github.com/couchbase/n1fty/flex"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"

	_ "github.com/blevesearch/bleve/config"
	_ "github.com/blevesearch/bleve/index/store/gtreap"
	_ "github.com/blevesearch/bleve/index/store/moss"
)

type Index struct {
	Parent  *Indexer
	IdStr   string
	NameStr string

	IndexMapping *mapping.IndexMappingImpl
	FlexIndex    *flex.FlexIndex
}

func (i *Index) KeyspaceId() string {
	return i.Parent.KeyspaceId()
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

func (i *Index) Statistics(requestId string, span *datastore.Span) (
	datastore.Statistics, errors.Error) {
	return nil, errors.NewError(nil, "not supported")
}

func (i *Index) Drop(requestId string) errors.Error {
	return errors.NewError(nil, "not supported")
}

func (i *Index) Scan(requestId string, span *datastore.Span, distinct bool, limit int64,
	cons datastore.ScanConsistency, vector timestamp.Vector, conn *datastore.IndexConnection) {
	conn.Error(errors.NewError(nil, "not supported"))
}

func (i *Index) Sargable(field string, query, options expression.Expression) (
	int, bool, errors.Error) {
	fmt.Printf("i.Sargable, %s, field: %s, query: %v, options: %v\n",
		i.IdStr, field, query, options)

	return 0, false, nil
}

// Search performs a search/scan over this index, with provided SearchInfo settings
func (i *Index) Search(requestId string, searchInfo *datastore.FTSSearchInfo,
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
		connError(errors.NewError(err, "bleve.NewMemOnly"))
		return
	}

	defer idx.Close()

	path := i.Parent.Keyspace.NamespaceId() + "/" +
		i.Parent.Keyspace.Id()

	entries, err1 := ioutil.ReadDir(path)
	if err1 != nil {
		connError(errors.NewError(err1, "ioutil.ReadDir"))
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		bytes, err2 := ioutil.ReadFile(path + "/" + entry.Name())
		if err2 != nil {
			connError(errors.NewError(err2, "ioutil.ReadFile"))
			return
		}

		var doc map[string]interface{}

		err = json.Unmarshal(bytes, &doc)
		if err != nil {
			connError(errors.NewError(err, "json.Unmarshal doc"))
			return
		}

		base := filepath.Base(entry.Name())
		id := base[0 : len(base)-len(filepath.Ext(base))]

		err = idx.Index(id, doc)
		if err != nil {
			connError(errors.NewError(err, "bleve Insert doc"))
			return
		}
	}

	qBytes, err3 := searchInfo.Query.MarshalJSON()
	if err3 != nil {
		connError(errors.NewError(err3, "searchInfo.Query.MarshalJSON"))
		return
	}

	q, err4 := query.ParseQuery(qBytes)
	if err4 != nil {
		connError(errors.NewError(err4, "query.ParseQuery"))
		return
	}

	sr := bleve.NewSearchRequest(q)

	res, err5 := idx.Search(sr)
	if err5 != nil {
		connError(errors.NewError(err5, "idx.Search"))
		return
	}

	entryCh := conn.EntryChannel()

	for _, hit := range res.Hits {
		entryCh <- &datastore.IndexEntry{PrimaryKey: hit.ID}
	}

	close(entryCh)
}

func (i *Index) Pageable(order []string, offset, limit int64) bool {
	fmt.Printf("i.Pageable, %s, order: %+v, offset: %v, limit: %v\n",
		i.IdStr, order, offset, limit)

	return false
}

// -----------------------------------------------------------------

func (i *Index) SargableFlex(nodeAlias string, bindings expression.Bindings,
	where expression.Expression) (sargLength int, exact bool,
	searchQuery, searchOptions map[string]interface{}, err errors.Error) {
	fmt.Printf("i.SargableFlex, nodeAlias: %s\n", nodeAlias)
	fmt.Printf("  where: %v\n", where)
	for _, b := range bindings {
		fmt.Printf("  binding: %+v, expr: %+v\n", b, b.Expression())
	}

	identifiers := flex.Identifiers{flex.Identifier{Name: nodeAlias}}

	var ok bool
	identifiers, ok = identifiers.Push(bindings, -1)
	if !ok {
		return 0, false, nil, nil, nil
	}

	fmt.Printf("  identifiers: %+v\n", identifiers)

	if i.FlexIndex == nil {
		return 0, false, nil, nil, nil
	}

	fieldTracks, needsFiltering, flexBuild, err0 := i.FlexIndex.Sargable(
		identifiers, where, nil)
	if err0 != nil {
		return 0, false, nil, nil, errors.NewError(err0, "")
	}

	if len(fieldTracks) <= 0 {
		return 0, false, nil, nil, nil
	}

	bleveQuery, err1 := flex.FlexBuildToBleveQuery(flexBuild, nil)
	if err1 != nil {
		return 0, false, nil, nil, errors.NewError(err1, "")
	}

	bleveQueryJ, _ := json.Marshal(bleveQuery)

	fmt.Printf("  bleveQuery: %s\n", bleveQueryJ)
	fmt.Printf("  fieldTracks: %v, needsFiltering: %v\n", fieldTracks, needsFiltering)

	return len(fieldTracks), !needsFiltering, bleveQuery, nil, nil
}
