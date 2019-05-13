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

// CBAUTH security/encryption seting
type securitySetting struct {
	encryptionEnabled bool
	disableNonSSLPort bool
	certificate       *tls.Certificate
	certInBytes       []byte
	tlsPreference     *cbauth.TLSConfig
}

var security unsafe.Pointer = unsafe.Pointer(new(securitySetting))

func getSecuritySetting() *securitySetting {
	return (*securitySetting)(atomic.LoadPointer(&security))
}

func updateSecuritySetting(s *securitySetting) {
	atomic.StorePointer(&security, unsafe.Pointer(s))
}

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 50 // 50 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 50 // 50 MB

var rsource rand.Source
var r1 *rand.Rand

func init() {
	rsource = rand.NewSource(time.Now().UnixNano())
	r1 = rand.New(rsource)
}

type ftsClient struct {
	gRPCConnMap map[string]*grpc.ClientConn
	serverMap   map[int]string
}

func (c *ftsClient) getGrpcClient() pb.SearchServiceClient {
	if len(c.serverMap) == 0 {
		return nil
	}
	index := r1.Intn(len(c.serverMap))
	conn := c.gRPCConnMap[c.serverMap[index]]
	return pb.NewSearchServiceClient(conn)
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
		gRPCConnMap: make(map[string]*grpc.ClientConn),
		serverMap:   make(map[int]string),
	}

	hosts, sslHosts := extractHosts(nodeDefs)
	securityCfg := getSecuritySetting()
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

	if securityCfg.encryptionEnabled && len(sslHosts) != 0 {
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(securityCfg.certInBytes)
		if !ok {
			return nil, fmt.Errorf("client: failed to append ca certs")
		}
		cred := credentials.NewClientTLSFromCert(certPool, "")
		gRPCOpts = append(gRPCOpts, grpc.WithTransportCredentials(cred))

		for i, hostPort := range sslHosts {
			client.serverMap[i] = hostPort
			cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
			if err != nil {
				return nil, fmt.Errorf("client: cbauth err: %v", err)
			}

			// copy
			opts := gRPCOpts[:]
			opts = append(opts, grpc.WithPerRPCCredentials(&basicAuthCreds{
				username:                 cbUser,
				password:                 cbPasswd,
				requireTransportSecurity: true,
			}))

			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				logging.Infof("client: grpc.Dial, err: %v", err)
				return nil, err
			}
			logging.Infof("client: grpc client connection created for host: %v", hostPort)
			client.gRPCConnMap[hostPort] = conn
		}
	} else if len(hosts) != 0 {
		gRPCOpts = append(gRPCOpts, grpc.WithInsecure())
		for i, hostPort := range hosts {
			client.serverMap[i] = hostPort
			cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
			if err != nil {
				return nil, fmt.Errorf("client: cbauth err: %v", err)
			}

			// copy
			opts := gRPCOpts[:]
			opts = append(opts, grpc.WithPerRPCCredentials(&basicAuthCreds{
				username: cbUser,
				password: cbPasswd,
			}))

			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				logging.Infof("client: grpc.Dial, err: %v", err)
				return nil, err
			}
			logging.Infof("client: grpc client connection created for host: %v", hostPort)
			client.gRPCConnMap[hostPort] = conn
		}
	}

	return client, nil
}

func extractHosts(nodeDefs *cbgt.NodeDefs) ([]string, []string) {
	hosts := []string{}
	sslHosts := []string{}

	for _, v := range nodeDefs.NodeDefs {
		extrasBindGRPC, err := v.GetFromParsedExtras("bindGRPC")
		if err == nil && extrasBindGRPC != nil {
			if bindGRPCstr, ok := extrasBindGRPC.(string); ok {
				if bindGRPCstr != "" {
					hosts = append(hosts, bindGRPCstr)
				}
			}
		}

		extrasBindGRPCSSL, err := v.GetFromParsedExtras("bindGRPCSSL")
		if err == nil && extrasBindGRPCSSL != nil {
			if bindGRPCSSLstr, ok := extrasBindGRPCSSL.(string); ok {
				if bindGRPCSSLstr != "" {
					sslHosts = append(sslHosts, bindGRPCSSLstr)
				}
			}
		}
	}

	return hosts, sslHosts
}
