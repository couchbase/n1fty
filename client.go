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
	"crypto/x509"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/query/logging"

	pb "github.com/couchbase/cbft/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 20 // 20 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 20 // 20 MB

var defaultPoolSize = int(1)

var rsource rand.Source
var r1 *rand.Rand

func init() {
	rsource = rand.NewSource(time.Now().UnixNano())
	r1 = rand.New(rsource)
}

type ftsSrvWrapper struct {
	m          sync.Mutex
	ftsGrpcEps map[string]interface{}
	connPool   map[string]*grpc.ClientConn // now only a single connection
	rrMap      map[int]string
	configs    map[string]interface{} // future
}

// singleton instance for handling all the
// grpc connection handling
var muclient sync.Mutex
var singletonClient *ftsSrvWrapper

func initWrapper(ftsEps map[string]interface{},
	cfg map[string]interface{}) (*ftsSrvWrapper, error) {
	muclient.Lock()
	if singletonClient == nil {
		singletonClient = &ftsSrvWrapper{
			configs:  cfg,
			connPool: make(map[string]*grpc.ClientConn),
			rrMap:    make(map[int]string),
		}
	}

	err := singletonClient.refresh(ftsEps)
	if err != nil {
		muclient.Unlock()
		return nil, err
	}

	muclient.Unlock()
	return singletonClient, nil
}

func (c *ftsSrvWrapper) getGrpcClient() pb.SearchServiceClient {
	c.m.Lock()
	index := r1.Intn(len(c.rrMap))
	hostPort, _ := c.rrMap[index]
	conn := c.connPool[hostPort]
	c.m.Unlock()
	return pb.NewSearchServiceClient(conn)
}

func (c *ftsSrvWrapper) refresh(ftsEps map[string]interface{}) error {
	c.m.Lock()
	defer c.m.Unlock()
	if ftsEps != nil {
		if reflect.DeepEqual(ftsEps, c.ftsGrpcEps) {
			return nil
		}
		c.ftsGrpcEps = ftsEps
	}

	connPool := make(map[string]*grpc.ClientConn, len(ftsEps))
	rrMap := make(map[int]string, len(ftsEps))
	var pos int
	for hostPort, certsPem := range c.ftsGrpcEps {
		rrMap[pos] = hostPort
		pos++
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM([]byte(certsPem.(string)))
		if !ok {
			return fmt.Errorf("client: failed to append ca certs")
		}
		cred := credentials.NewClientTLSFromCert(certPool, "")

		cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
		if err != nil {
			return fmt.Errorf("client: cbauth err: %v", err)
		}

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(cred),

			// TODO: grpc.WithInsecure() ?

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

			grpc.WithPerRPCCredentials(&basicAuthCreds{
				username: cbUser,
				password: cbPasswd,
			}),
		}

		if gconn, initialised := c.connPool[hostPort]; !initialised {
			for i := 0; i < defaultPoolSize; i++ {
				conn, err := grpc.Dial(hostPort, opts...)
				if err != nil {
					logging.Infof("client: grpc.Dial, err: %v", err)
					return err
				}
				logging.Infof("client: %d grpc ClientConn Created for host %s",
					hostPort, i+1)
				connPool[hostPort] = conn
			}
		} else {
			// take the already established connection
			connPool[hostPort] = gconn
		}
	}

	c.connPool = connPool
	c.rrMap = rrMap
	return nil
}

func extractHostCertsMap(nodeDefs *cbgt.NodeDefs) (map[string]interface{}, error) {
	if nodeDefs == nil {
		return nil, nil
	}

	hostCertsMap := make(map[string]interface{}, 2)
	var host string
	for _, v := range nodeDefs.NodeDefs {
		extrasBindGRPC, er := v.GetFromParsedExtras("bindGRPCSSL")
		if er == nil && extrasBindGRPC != nil {
			if bindGRPCstr, ok := extrasBindGRPC.(string); ok {
				host = bindGRPCstr
			}
		}

		if host != "" {
			hostCertsMap[host] = nil
			extrasCertPEM, er := v.GetFromParsedExtras("tlsCertPEM")
			if er == nil && extrasCertPEM != nil {
				hostCertsMap[host] = extrasCertPEM
			}
		}
	}

	return hostCertsMap, nil
}
