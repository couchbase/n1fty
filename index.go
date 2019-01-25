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
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/timestamp"
)

// Implements datastore.Index interface
type FTSIndex struct {
	indexer  *FTSIndexer
	id       string
	name     string
	indexDef *cbgt.IndexDef

	typeMappings        []string
	rangeKeyExpressions expression.Expressions
}

// ----------------------------------------------------------------------------

func newFTSIndex(fieldTypeMap map[string][]string, indexDef *cbgt.IndexDef,
	indexer *FTSIndexer) (*FTSIndex, errors.Error) {
	index := &FTSIndex{
		indexer:            indexer,
		id:                 indexDef.UUID,
		name:               indexDef.Name,
		indexDef:           indexDef,
		typeMappings:       []string{},
		rangeKeyExpression: make(expression.Expressions),
	}

	for typeName, fields := range fieldTypeMap {
		for _, entry := range fields {
			rangeKeyExpr, err := parser.Parse(entry)
			if err != nil {
				return nil, errors.NewError(err, "failed to build expressions")
			}
			index.rangeKeyExpressions = append(index.rangeKeyExpressions, rangeKeyExpr)
		}
		index.typeMappings = append(index.typeMappings, typeName)
	}

	return index, nil
}

// ----------------------------------------------------------------------------

func (i *FTSIndex) KeyspaceId() string {
	return i.indexer.KeySpaceId()
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

// FIXME
// Perform a search/scan over this index, with provided SearchInfo settings
func (i *FTSIndex) Search(requestId string, searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
}
