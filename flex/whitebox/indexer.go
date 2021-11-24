//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

func (i *Indexer) BucketId() string {
	return ""
}

func (i *Indexer) ScopeId() string {
	return ""
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

func (i *Indexer) CreatePrimaryIndex(requestID, name string,
	with value.Value) (datastore.PrimaryIndex, errors.Error) {
	return nil, util.N1QLError(nil, "not supported")
}

func (i *Indexer) CreateIndex(requestID, name string,
	seekKey, rangeKey expression.Expressions, where expression.Expression,
	with value.Value) (datastore.Index, errors.Error) {
	return nil, util.N1QLError(nil, "not supported")
}

func (i *Indexer) BuildIndexes(requestID string, names ...string) errors.Error {
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

func (i *Indexer) SetConnectionSecurityConfig(config *datastore.ConnectionSecurityConfig) {
	// TODO
}

// -------------------------------------------------

func MakeWrapCallbacksForIndexType(indexType datastore.IndexType,
	initIndexer func(*Indexer) (*Indexer, errors.Error)) *WrapCallbacks {
	if initIndexer == nil {
		initIndexer = func(i *Indexer) (*Indexer, errors.Error) {
			return i, nil
		}
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
