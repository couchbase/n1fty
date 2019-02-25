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
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"
)

type Indexer struct {
	Keyspace    datastore.Keyspace
	IndexType   datastore.IndexType
	IndexesById map[string]*Index
}

func (i *Indexer) KeyspaceId() string {
	return i.Keyspace.Id()
}

func (i *Indexer) Name() datastore.IndexType {
	return i.IndexType
}

func (i *Indexer) IndexIds() (rv []string, err errors.Error) {
	for id := range i.IndexesById {
		rv = append(rv, id)
	}
	return rv, nil
}

func (i *Indexer) IndexNames() (rv []string, err errors.Error) {
	for _, idx := range i.IndexesById {
		rv = append(rv, idx.Name())
	}
	return rv, nil
}

func (i *Indexer) IndexById(id string) (datastore.Index, errors.Error) {
	idx, ok := i.IndexesById[id]
	if !ok {
		return nil, errors.NewOtherIdxNotFoundError(nil, id)
	}
	return idx, nil
}

func (i *Indexer) IndexByName(name string) (datastore.Index, errors.Error) {
	for _, idx := range i.IndexesById {
		if idx.Name() == name {
			return idx, nil
		}
	}
	return nil, errors.NewOtherIdxNotFoundError(nil, name)
}

func (i *Indexer) PrimaryIndexes() ([]datastore.PrimaryIndex, errors.Error) {
	return nil, nil
}

func (i *Indexer) Indexes() (rv []datastore.Index, err errors.Error) {
	for _, idx := range i.IndexesById {
		rv = append(rv, idx)
	}
	return rv, nil
}

func (i *Indexer) CreatePrimaryIndex(requestId, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, util.N1QLError(nil, "not supported")
}

func (i *Indexer) CreateIndex(requestId, name string,
	seekKey, rangeKey expression.Expressions, where expression.Expression,
	with value.Value) (datastore.Index, errors.Error) {
	return nil, util.N1QLError(nil, "not supported")
}

func (i *Indexer) BuildIndexes(requestId string, names ...string) errors.Error {
	return util.N1QLError(nil, "not supported")
}

func (i *Indexer) Refresh() errors.Error {
	return nil
}

func (i *Indexer) MetadataVersion() uint64 {
	return 0
}

func (i *Indexer) SetLogLevel(level logging.Level) {
}

// -------------------------------------------------

func MakeWrapCallbacksForIndexType(indexType datastore.IndexType,
	initIndexer func(*Indexer) (*Indexer, errors.Error)) *WrapCallbacks {
	if initIndexer == nil {
		initIndexer = func(i *Indexer) (*Indexer, errors.Error) { return i, nil }
	}

	return &WrapCallbacks{
		KeyspaceIndexer: func(b *WrapKeyspace, name datastore.IndexType) (datastore.Indexer, errors.Error) {
			indexer, err := b.W.Indexer(name)
			if err != nil {
				return nil, err
			}

			if indexer == nil && name == indexType {
				return initIndexer(&Indexer{
					Keyspace:  b,
					IndexType: indexType,
				})
			}

			return indexer, nil
		},

		KeyspaceIndexers: func(b *WrapKeyspace) ([]datastore.Indexer, errors.Error) {
			indexers, err := b.W.Indexers()
			if err != nil {
				return nil, err
			}

			for _, indexer := range indexers {
				if indexer.Name() == indexType {
					return indexers, nil
				}
			}

			indexer, err := initIndexer(&Indexer{
				Keyspace:  b,
				IndexType: indexType,
			})

			return append(append([]datastore.Indexer(nil), indexers...),
				indexer), err
		},
	}
}
