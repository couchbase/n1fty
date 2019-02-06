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
	"fmt"
	"time"

	"github.com/couchbase/cbauth"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/builder"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 20 // 20 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 20 // 20 MB

// -----------------------------------------------------------------------------

// Implements datastore.FTSIndex interface
type FTSIndex struct {
	indexer  *FTSIndexer
	id       string
	name     string
	indexDef *cbgt.IndexDef

	searchableFieldsMap map[string][]string // map of type to fields
	rangeKeyExpressions expression.Expressions
}

// -----------------------------------------------------------------------------

func newFTSIndex(searchableFieldsMap map[string][]string, indexDef *cbgt.IndexDef,
	indexer *FTSIndexer) (*FTSIndex, error) {
	index := &FTSIndex{
		indexer:             indexer,
		id:                  indexDef.UUID,
		name:                indexDef.Name,
		indexDef:            indexDef,
		searchableFieldsMap: searchableFieldsMap,
		rangeKeyExpressions: expression.Expressions{},
	}

	for _, fields := range searchableFieldsMap {
		for _, entry := range fields {
			rangeKeyExpr, err := parser.Parse(entry)
			if err != nil {
				return nil, err
			}
			index.rangeKeyExpressions = append(index.rangeKeyExpressions,
				rangeKeyExpr)
		}
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

// -----------------------------------------------------------------------------

// Perform a search/scan over this index, with provided SearchInfo settings
func (i *FTSIndex) Search(requestId string, searchInfo *datastore.FTSSearchInfo,
	cons datastore.ScanConsistency, vector timestamp.Vector,
	conn *datastore.IndexConnection) {
	if conn == nil {
		return
	}

	if searchInfo == nil {
		conn.Error(errors.NewError(nil, "no search parameters provided"))
		return
	}

	username, password, err := cbauth.GetHTTPServiceAuth(i.indexer.serverURL)
	if err != nil {
		conn.Error(errors.NewError(nil, "error fetching auth creds"))
		return
	}

	options := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// timeout value for an inactive connection
			Timeout: DefaultGrpcConnectionIdleTimeout,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGrpcMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGrpcMaxSendMsgSize),
		),
		grpc.WithPerRPCCredentials(&basicAuthCreds{
			username: username,
			password: password,
		}),
	}

	grpcConn, err := grpc.Dial(i.indexer.serverURL, options...)
	if err != nil {
		conn.Error(errors.NewError(err, "could not connect to gRPC port"))
		return
	}
	defer grpcConn.Close()

	// create a new customer client
	client := pb.NewSearchServiceClient(grpcConn)

	query, err := builder.BuildQueryBytes(searchInfo.Field.String(),
		searchInfo.Query.String(),
		searchInfo.Options.String())
	if err != nil {
		conn.Error(errors.NewError(err, ""))
		return
	}

	searchRequest := &pb.SearchRequest{
		Query:     query,
		Stream:    true,
		From:      searchInfo.Offset,
		Size:      searchInfo.Limit,
		IndexName: i.name,
	}

	stream, err := client.Search(context.Background(), searchRequest)
	if err != nil || stream == nil {
		conn.Error(errors.NewError(err, "search failed"))
		return
	}

	fmt.Println(stream)
	// FIXME
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Sargable(field string, query, options value.Value) (
	int, bool, errors.Error) {
	// FIXME
	return 0, false, nil
}

// -----------------------------------------------------------------------------

func (i *FTSIndex) Pagination(order []string, offset, limit uint64) bool {
	// FIXME
	return false
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

func getTmpSpaceDir() string {
	conf := config.GetConfig()

	if conf == nil {
		return getDefaultTmpDir()
	}

	if v, ok := conf[tmpSpaceDir]; ok {
		return v.(string)
	}

	return getDefaultTmpDir()
}

func getTmpSpaceLimit() int64 {
	conf := config.GetConfig()

	if conf == nil {
		return defaultBackfillLimit
	}

	if v, ok := conf[tmpSpaceLimit]; ok {
		return v.(int64)
	}

	return defaultBackfillLimit
}