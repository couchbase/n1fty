//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package whitebox

import (
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

func (s *WrapDatastore) Authorize(p *auth.Privileges, c *auth.Credentials) errors.Error {
	return s.W.Authorize(p, c)
}

func (s *WrapDatastore) AdminUser(node string) (string, string, error) {
	return "", "", nil
}

func (s *WrapDatastore) PreAuthorize(p *auth.Privileges) {
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

func (s *WrapDatastore) StartTransaction(stmtAtomicity bool, context datastore.QueryContext) (map[string]bool, errors.Error) {
	return nil, errors.NewTranDatastoreNotSupportedError("file")
}

func (s *WrapDatastore) CommitTransaction(stmtAtomicity bool, context datastore.QueryContext) errors.Error {
	return errors.NewTranDatastoreNotSupportedError("file")
}

func (s *WrapDatastore) RollbackTransaction(stmtAtomicity bool, context datastore.QueryContext, sname string) errors.Error {
	return errors.NewTranDatastoreNotSupportedError("file")
}

func (s *WrapDatastore) SetSavepoint(stmtAtomicity bool, context datastore.QueryContext, sname string) errors.Error {
	return errors.NewTranDatastoreNotSupportedError("file")
}

func (s *WrapDatastore) TransactionDeltaKeyScan(keyspace string, conn *datastore.IndexConnection) {
	defer conn.Sender().Close()
}

func (s *WrapDatastore) CreateSystemCBOStats(requestId string) errors.Error {
	return nil
}

func (s *WrapDatastore) DropSystemCBOStats() errors.Error {
	return nil
}

func (s *WrapDatastore) GetSystemCBOStats() (datastore.Keyspace, errors.Error) {
	return nil, nil
}

func (s *WrapDatastore) HasSystemCBOStats() (bool, errors.Error) {
	return false, nil
}

func (s *WrapDatastore) GetSystemCollection(bucketName string) (datastore.Keyspace, errors.Error) {
	return nil, nil
}

func (s *WrapDatastore) CheckSystemCollection(bucketName, requestId string) errors.Error {
	return nil
}

func (s *WrapDatastore) EnableStorageAudit(val bool) {

}

func (s *WrapDatastore) GetUserBuckets(c *auth.Credentials) []string {
	return []string{}
}

func (s *WrapDatastore) GetImpersonateBuckets(string, string) []string {
	return []string{}
}

func (s *WrapDatastore) CredsString(*auth.Credentials) (string, string) {
	return "", ""
}

func (s *WrapDatastore) GetUserUUID(creds *auth.Credentials) string {
	return ""
}

// ------------------------------------------------------------------------

type WrapNamespace struct {
	Parent *WrapDatastore
	W      datastore.Namespace
}

func (p *WrapNamespace) Datastore() datastore.Datastore {
	return p.Parent
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

func (p *WrapNamespace) Objects(credentials *auth.Credentials, filter func(string) bool, preload bool) (
	[]datastore.Object, errors.Error) {
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
	context datastore.QueryContext, subPaths []string, projection []string, useSubDoc bool) errors.Errors {
	return b.W.Fetch(keys, keysMap, context, subPaths, projection, useSubDoc)
}

func (b *WrapKeyspace) Insert(inserts value.Pairs, context datastore.QueryContext, preserveMutations bool) (int, value.Pairs, errors.Errors) {
	return b.W.Insert(inserts, context, preserveMutations)
}

func (b *WrapKeyspace) Update(updates value.Pairs, context datastore.QueryContext, preserveMutations bool) (int, value.Pairs, errors.Errors) {
	return b.W.Update(updates, context, preserveMutations)
}

func (b *WrapKeyspace) Upsert(upserts value.Pairs, context datastore.QueryContext, preserveMutations bool) (int, value.Pairs, errors.Errors) {
	return b.W.Upsert(upserts, context, preserveMutations)
}

func (b *WrapKeyspace) Delete(deletes value.Pairs, context datastore.QueryContext, preserveMutations bool) (int, value.Pairs, errors.Errors) {
	return b.W.Delete(deletes, context, preserveMutations)
}

func (b *WrapKeyspace) SetSubDoc(key string, elems value.Pairs, context datastore.QueryContext) (value.Pairs, errors.Error) {
	return b.W.SetSubDoc(key, elems, context)
}

func (b *WrapKeyspace) Release(close bool) {
	b.W.Release(close)
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

func (b *WrapKeyspace) Uid() string {
	return ""
}

func (b *WrapKeyspace) Stats(ctx datastore.QueryContext,
	which []datastore.KeyspaceStats) ([]int64, errors.Error) {
	return []int64{}, nil
}
