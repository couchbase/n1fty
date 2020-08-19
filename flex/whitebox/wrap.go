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
	"net/http"

	"github.com/couchbase/query/auth"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"
)

type WrapCallbacks struct {
	KeyspaceIndexers func(w *WrapKeyspace) (
		[]datastore.Indexer, errors.Error)

	KeyspaceIndexer func(w *WrapKeyspace, name datastore.IndexType) (
		datastore.Indexer, errors.Error)
}

// ------------------------------------------------------------------------

type WrapDatastore struct {
	W datastore.Datastore
	C *WrapCallbacks
}

func (s *WrapDatastore) Id() string {
	return s.W.Id()
}

func (s *WrapDatastore) URL() string {
	return s.W.URL()
}

func (s *WrapDatastore) Info() datastore.Info {
	return s.W.Info()
}

func (s *WrapDatastore) NamespaceIds() ([]string, errors.Error) {
	return s.W.NamespaceIds()
}

func (s *WrapDatastore) NamespaceNames() ([]string, errors.Error) {
	return s.W.NamespaceNames()
}

func (s *WrapDatastore) NamespaceById(id string) (p datastore.Namespace, e errors.Error) {
	ns, err := s.W.NamespaceById(id)
	return &WrapNamespace{Parent: s, W: ns}, err
}

func (s *WrapDatastore) NamespaceByName(name string) (p datastore.Namespace, e errors.Error) {
	ns, err := s.W.NamespaceByName(name)
	return &WrapNamespace{Parent: s, W: ns}, err
}

func (s *WrapDatastore) Authorize(p *auth.Privileges, c *auth.Credentials) (auth.AuthenticatedUsers, errors.Error) {
	return s.W.Authorize(p, c)
}

func (s *WrapDatastore) CredsString(r *http.Request) string {
	return s.W.CredsString(r)
}

func (s *WrapDatastore) SetLogLevel(level logging.Level) {
	s.W.SetLogLevel(level)
}

func (s *WrapDatastore) Inferencer(name datastore.InferenceType) (datastore.Inferencer, errors.Error) {
	return s.W.Inferencer(name)
}

func (s *WrapDatastore) Inferencers() ([]datastore.Inferencer, errors.Error) {
	return s.W.Inferencers()
}

func (s *WrapDatastore) AuditInfo() (*datastore.AuditInfo, errors.Error) {
	return s.W.AuditInfo()
}

func (s *WrapDatastore) ProcessAuditUpdateStream(callb func(uid string) error) errors.Error {
	return s.W.ProcessAuditUpdateStream(callb)
}

func (s *WrapDatastore) UserInfo() (value.Value, errors.Error) {
	return s.W.UserInfo()
}

func (s *WrapDatastore) GetUserInfoAll() ([]datastore.User, errors.Error) {
	return s.W.GetUserInfoAll()
}

func (s *WrapDatastore) PutUserInfo(u *datastore.User) errors.Error {
	return s.W.PutUserInfo(u)
}

func (s *WrapDatastore) GetRolesAll() ([]datastore.Role, errors.Error) {
	return s.W.GetRolesAll()
}

func (s *WrapDatastore) StatUpdater() (datastore.StatUpdater, errors.Error) {
	// TODO fix the place holder impl
	return nil, nil
}

func (s *WrapDatastore) SetConnectionSecurityConfig(config *datastore.ConnectionSecurityConfig) {
}

func (s *WrapDatastore) CreateSystemCBOStats(requestId string) errors.Error {
	return nil
}

func (s *WrapDatastore) GetSystemCBOStats() (datastore.Keyspace, errors.Error) {
	return nil, nil
}

func (s *WrapDatastore) HasSystemCBOStats() (bool, errors.Error) {
	return false, nil
}

// ------------------------------------------------------------------------

type WrapNamespace struct {
	Parent *WrapDatastore
	W      datastore.Namespace
}

func (p *WrapNamespace) DatastoreId() string {
	return p.W.DatastoreId()
}

func (p *WrapNamespace) Id() string {
	return p.W.Id()
}

func (p *WrapNamespace) Name() string {
	return p.W.Name()
}

func (p *WrapNamespace) KeyspaceIds() ([]string, errors.Error) {
	return p.W.KeyspaceIds()
}

func (p *WrapNamespace) KeyspaceNames() ([]string, errors.Error) {
	return p.W.KeyspaceNames()
}

func (p *WrapNamespace) KeyspaceById(id string) (b datastore.Keyspace, e errors.Error) {
	ks, err := p.W.KeyspaceById(id)
	return &WrapKeyspace{Parent: p, W: ks}, err
}

func (p *WrapNamespace) KeyspaceByName(name string) (b datastore.Keyspace, e errors.Error) {
	ks, err := p.W.KeyspaceByName(name)
	return &WrapKeyspace{Parent: p, W: ks}, err
}

func (p *WrapNamespace) MetadataVersion() uint64 {
	return 0
}

func (p *WrapNamespace) BucketIds() ([]string, errors.Error) {
	return p.W.BucketIds()
}

func (p *WrapNamespace) BucketNames() ([]string, errors.Error) {
	return p.W.BucketNames()
}

func (p *WrapNamespace) BucketById(id string) (datastore.Bucket, errors.Error) {
	return p.W.BucketById(id)
}

func (p *WrapNamespace) BucketByName(name string) (datastore.Bucket, errors.Error) {
	return p.W.BucketByName(name)
}

func (p *WrapNamespace) Objects() ([]datastore.Object, errors.Error) {
	return nil, nil
}

// ------------------------------------------------------------------------

type WrapKeyspace struct {
	Parent *WrapNamespace
	W      datastore.Keyspace
}

func (b *WrapKeyspace) NamespaceId() string {
	return b.W.NamespaceId()
}

func (b *WrapKeyspace) Namespace() datastore.Namespace {
	return b.W.Namespace()
}

func (b *WrapKeyspace) ScopeId() string {
	return b.W.ScopeId()
}

func (b *WrapKeyspace) Scope() datastore.Scope {
	return b.W.Scope()
}

func (b *WrapKeyspace) Id() string {
	return b.W.Id()
}

func (b *WrapKeyspace) Name() string {
	return b.W.Name()
}

func (p *WrapKeyspace) QualifiedName() string {
	return ""
}

func (p *WrapKeyspace) AuthKey() string {
	return ""
}

func (b *WrapKeyspace) Count(context datastore.QueryContext) (int64, errors.Error) {
	return b.W.Count(context)
}

func (b *WrapKeyspace) Indexer(name datastore.IndexType) (datastore.Indexer, errors.Error) {
	if b.Parent.Parent.C != nil &&
		b.Parent.Parent.C.KeyspaceIndexer != nil {
		return b.Parent.Parent.C.KeyspaceIndexer(b, name)
	}

	return b.W.Indexer(name)
}

func (b *WrapKeyspace) Indexers() ([]datastore.Indexer, errors.Error) {
	if b.Parent.Parent.C != nil &&
		b.Parent.Parent.C.KeyspaceIndexers != nil {
		return b.Parent.Parent.C.KeyspaceIndexers(b)
	}

	return b.W.Indexers()
}

func (b *WrapKeyspace) Fetch(keys []string, keysMap map[string]value.AnnotatedValue,
	context datastore.QueryContext, subPaths []string) []errors.Error {
	return b.W.Fetch(keys, keysMap, context, subPaths)
}

func (b *WrapKeyspace) Insert(inserts []value.Pair) ([]value.Pair, errors.Error) {
	return b.W.Insert(inserts)
}

func (b *WrapKeyspace) Update(updates []value.Pair) ([]value.Pair, errors.Error) {
	return b.W.Update(updates)
}

func (b *WrapKeyspace) Upsert(upserts []value.Pair) ([]value.Pair, errors.Error) {
	return b.W.Upsert(upserts)
}

func (b *WrapKeyspace) Delete(deletes []string, context datastore.QueryContext) ([]string, errors.Error) {
	return b.W.Delete(deletes, context)
}

func (b *WrapKeyspace) Release() {
	b.W.Release()
}

func (b *WrapKeyspace) IsBucket() bool {
	return true
}

func (b *WrapKeyspace) Size(ctx datastore.QueryContext) (int64, errors.Error) {
	// TODO fix the place holder impl
	return b.W.Size(ctx)
}

func (b *WrapKeyspace) Flush() errors.Error {
	return nil
}
