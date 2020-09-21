// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package n1fty

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/logging"

	pb "github.com/couchbase/cbft/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// CBAUTH security/encryption config
type securityConfig struct {
	encryptionEnabled bool
	disableNonSSLPort bool
	certificate       *tls.Certificate
	certInBytes       []byte
	tlsPreference     *cbauth.TLSConfig
}

var secConfig = unsafe.Pointer(new(securityConfig))

func loadSecurityConfig() *securityConfig {
	return (*securityConfig)(atomic.LoadPointer(&secConfig))
}

func updateSecurityConfig(sc *securityConfig) {
	atomic.StorePointer(&secConfig, unsafe.Pointer(sc))
}

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 50 // 50 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 50 // 50 MB

// DefaultConnPoolSize decides the connection pool size per host
var DefaultConnPoolSize = 5

// ErrFeatureUnavailable indicates the feature unavailability in cluster
var ErrFeatureUnavailable = fmt.Errorf("feature unavailable in cluster")

var rsource rand.Source
var r1 *rand.Rand

func init() {
	rsource = rand.NewSource(time.Now().UnixNano())
	r1 = rand.New(rsource)
}

type ftsClient struct {
	gRPCConnMap map[string][]*grpc.ClientConn
	serverMap   map[int]string
}

func (c *ftsClient) getGrpcClient() pb.SearchServiceClient {
	if len(c.serverMap) == 0 {
		return nil
	}
	// pick a random fts node
	randomNodeIndex := r1.Intn(len(c.serverMap))
	// pick its conn pool
	connPool := c.gRPCConnMap[c.serverMap[randomNodeIndex]]
	if len(connPool) == 0 {
		return nil
	}
	// pick a random connection from pool
	conn := connPool[r1.Intn(len(connPool))]
	return pb.NewSearchServiceClient(conn)
}

func (c *ftsClient) initConnections(hosts []string,
	options []grpc.DialOption, secure bool) error {
	if len(hosts) == 0 {
		return ErrFeatureUnavailable
	}

OUTER:
	for i, hostPort := range hosts {
		cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
		if err != nil {
			// it is possible that some hosts may be unreachable during
			// a cluster operation, so ignore error here; see: MB-40125
			continue
		}

		opts := options[:]
		opts = append(opts, grpc.WithPerRPCCredentials(&basicAuthCreds{
			username:                 cbUser,
			password:                 cbPasswd,
			requireTransportSecurity: secure,
		}))

		for j := 0; j < DefaultConnPoolSize; j++ {
			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				logging.Infof("client: grpc.Dial for host: %s, err: %v", hostPort, err)
				continue OUTER
			}
			logging.Infof("client: grpc client connection #%d created for host: %v", j, hostPort)
			c.gRPCConnMap[hostPort] = append(c.gRPCConnMap[hostPort], conn)
		}
		// after making the connections ready, update the serverInfo in map
		c.serverMap[i] = hostPort
	}
	return nil
}

func (c *ftsClient) Close() {
	for _, conns := range c.gRPCConnMap {
		for i := 0; i < len(conns); i++ {
			conns[i].Close()
		}
	}
}

// -----------------------------------------------------------------------------

// basicAuthCreds is an implementation of credentials.PerRPCCredentials
// that transforms the username and password into a base64 encoded value
// similar to HTTP Basic xxx
type basicAuthCreds struct {
	username                 string
	password                 string
	requireTransportSecurity bool
}

// GetRequestMetadata sets the value for "authorization" key
func (b *basicAuthCreds) GetRequestMetadata(context.Context, ...string) (
	map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + basicAuth(b.username, b.password),
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires
// transport security.
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return b.requireTransportSecurity
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

// -----------------------------------------------------------------------------

func setupFTSClient(nodeDefs *cbgt.NodeDefs) (*ftsClient, error) {
	if nodeDefs == nil {
		return nil, nil
	}

	client := &ftsClient{
		gRPCConnMap: make(map[string][]*grpc.ClientConn),
		serverMap:   make(map[int]string),
	}

	hosts, sslHosts := extractHosts(nodeDefs)
	if len(hosts) == 0 && len(sslHosts) == 0 {
		return nil, ErrFeatureUnavailable
	}

	secConfig := loadSecurityConfig()
	gRPCOpts := []grpc.DialOption{
		grpc.WithBackoffMaxDelay(DefaultGrpcMaxBackOffDelay),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// send keepalive every N seconds to check the
			// connection liveliness
			Time: DefaultGrpcConnectionHeartBeatInterval,
			// client waits for a duration of timeout
			Timeout: DefaultGrpcConnectionIdleTimeout,

			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGrpcMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGrpcMaxSendMsgSize),
		),
		// TODO: addClientInterceptor() ?
	}

	if secConfig.encryptionEnabled && len(sslHosts) != 0 {
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(secConfig.certInBytes)
		if !ok {
			return nil, fmt.Errorf("client: failed to append ca certs")
		}
		cred := credentials.NewClientTLSFromCert(certPool, "")
		gRPCOpts = append(gRPCOpts, grpc.WithTransportCredentials(cred))
		hosts = sslHosts
	} else if len(hosts) > 0 {
		gRPCOpts = append(gRPCOpts, grpc.WithInsecure())
	}

	err := client.initConnections(hosts, gRPCOpts,
		secConfig.encryptionEnabled)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func extractHosts(nodeDefs *cbgt.NodeDefs) ([]string, []string) {
	hosts := []string{}
	sslHosts := []string{}

	for _, v := range nodeDefs.NodeDefs {
		var grpcFeatureSupport bool
		extrasBindGRPC, err := v.GetFromParsedExtras("bindGRPC")
		if err == nil && extrasBindGRPC != nil {
			if bindGRPCstr, ok := extrasBindGRPC.(string); ok {
				if bindGRPCstr != "" {
					hosts = append(hosts, bindGRPCstr)
					grpcFeatureSupport = true
				}
			}
		}

		extrasBindGRPCSSL, err := v.GetFromParsedExtras("bindGRPCSSL")
		if err == nil && extrasBindGRPCSSL != nil {
			if bindGRPCSSLstr, ok := extrasBindGRPCSSL.(string); ok {
				if bindGRPCSSLstr != "" {
					sslHosts = append(sslHosts, bindGRPCSSLstr)
					grpcFeatureSupport = true
				}
			}
		}
		// if any node in the cluster doesn't support gRPC then fail right away
		if !grpcFeatureSupport {
			return nil, nil
		}
	}

	return hosts, sslHosts
}
