// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package n1fty

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/logging"

	pb "github.com/couchbase/cbft/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// CBAUTH security/encryption config
type securityConfig struct {
	encryptionEnabled  bool
	disableNonSSLPorts bool
	certificate        *tls.Certificate
	certInBytes        []byte
	tlsPreference      *cbauth.TLSConfig
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

// ErrFeatureUnavailable indicates the feature unavailability in cluster
var ErrFeatureUnavailable = fmt.Errorf("feature unavailable in cluster")

type connectionType int

const (
	sslConn connectionType = iota
	nonSslConn
)

func init() {
	rand.Seed(time.Now().UnixNano())

	ftsClientInst = &ftsClient{
		connPool: make(map[string]*connPool),
		connOpts: newGrpcOpts(),
	}
}

type ftsClient struct {
	connOpts *grpcOpts
	servers  []string // cluster's hosts ( used for reconcilation)

	// a write-lock on this mutex will make all new queries to wait.
	availHostsMutex sync.RWMutex
	availableHosts  []string // cluster's reachable hosts (creds available)

	connPoolMutex sync.Mutex // serialize updates to connPool
	// A connPool per host, these are shadowed under availableHosts.
	// A query will only be served from a connPool if its host is available.
	connPool map[string]*connPool
}

// Instance of ftsClient
var ftsClientInst *ftsClient

func getUpdateReason(isSecCfgChanged bool) string {
	if isSecCfgChanged {
		return "SecurityConfigChange"
	}
	return "TopologyChange"
}

// This function deals with events of indexers count crossing 0 to 1 and vice-versa
//
// param @indexersExist: passed as true if indexers count went from 0 to 1 and vice-versa
func (c *ftsClient) updateConnPoolsOnIndexersChange(indexersExist bool) {
	c.connPoolMutex.Lock()
	defer c.connPoolMutex.Unlock()
	defer func() {
		logging.Infof("n1fty: updateConnPools done, event:IndexerExist-%v, "+
			"servers:%v, avaiableHosts:%v", indexersExist, c.servers,
			c.availableHosts)
	}()

	logging.Infof("n1fty: updateConnPools start, event: IndexerExist-%v",
		indexersExist)

	secConfig := loadSecurityConfig()
	// previously unavailable hosts which became available
	nowAvailableHosts := c.connOpts.updateHostsAvailability(secConfig)
	c.availHostsMutex.Lock()
	c.availableHosts = append(c.availableHosts, nowAvailableHosts...)
	c.availHostsMutex.Unlock()

	// Indexers existence toggled from 0 to 1
	if indexersExist {
		for _, host := range c.availableHosts {
			c.connPool[host] = newConnPool(host)
		}
		return
	}

	// Indexers existence toggled from 1 to 0
	// close all conn pools
	for host, _ := range c.connPool {
		c.connPool[host].close("NoIndexerExist")
		delete(c.connPool, host)
	}
}

func (c *ftsClient) updateConnPools(isSecCfgChanged bool) error {
	c.connPoolMutex.Lock()
	defer c.connPoolMutex.Unlock()

	indexersExists := false
	mr.m.RLock()
	indexersExists = len(mr.indexers) != 0
	mr.m.RUnlock()

	defer func() {
		logging.Infof("n1fty: updateConnPools done, event:%v, servers:%v, "+
			"avaiableHosts:%v", getUpdateReason(isSecCfgChanged), c.servers,
			c.availableHosts)
	}()

	logging.Infof("n1fty: updateConnPools start, event:%v, indexersExist:%v",
		getUpdateReason(isSecCfgChanged), indexersExists)
	return c.updateConnPoolsLOCKED(isSecCfgChanged, indexersExists)
}

// This function updates (or creates) connection pools based on events like
//   - TopologyChange
//   - SecurityConfigChange
//
// Effect on on-going requests:
//   - TopologyChange: Ongoing reqs to removed nodes will be failed
//   - SecurityConfigChange: All ongoing reqs will be failed
func (c *ftsClient) updateConnPoolsLOCKED(isSecCfgChanged, indexersExists bool) error {
	// ## Fetch new topology and reconcile with existing topology

	nodeDefs, err := GetNodeDefs(srvConfig)
	if err != nil {
		logging.Errorf("n1fty: GetNodeDefs, err: %v", err)
		return fmt.Errorf("GetNodeDefs failed [err: %v]", err)
	}

	if nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		return fmt.Errorf("hosts (ssl/non-ssl) unavailable")
	}

	hosts, sslHosts := extractHosts(nodeDefs)
	if len(hosts) == 0 && len(sslHosts) == 0 {
		return ErrFeatureUnavailable
	}

	var newTopology []string
	var connType connectionType

	secConfig := loadSecurityConfig()
	if secConfig.encryptionEnabled && len(sslHosts) > 0 {
		newTopology = sslHosts
		connType = sslConn
	} else if !secConfig.disableNonSSLPorts && len(hosts) > 0 {
		newTopology = hosts // non-ssl hosts
		connType = nonSslConn
	} else {
		logging.Warnf("n1fty: client: hosts error, config: {encryptionEnabled:"+
			" %v, disableNonSSLPorts: %v}, hosts: %v, sslHosts: %v",
			secConfig.encryptionEnabled, secConfig.disableNonSSLPorts,
			hosts, sslHosts)
		return fmt.Errorf("hosts (ssl/non-ssl) unavailable")
	}

	// reconcile topology
	keepHosts := cbgt.StringsIntersectStrings(c.servers, newTopology)
	removedHosts := cbgt.StringsRemoveStrings(c.servers, newTopology)
	addedHosts := cbgt.StringsRemoveStrings(newTopology, keepHosts)

	if len(addedHosts) != 0 || len(removedHosts) != 0 {
		c.servers = newTopology
	}

	// ## Handle SecurityConfigChange Event

	if isSecCfgChanged {
		availableHosts, err := c.connOpts.update(
			newTopology, connType, isSecCfgChanged, secConfig)

		if err != nil {
			logging.Infof("n1fty: failed to create/update connOpts for "+
				"hosts:%+v, err: %v", newTopology, err)
			c.availHostsMutex.Lock()
			c.availableHosts = nil
			c.availHostsMutex.Unlock()
		}

		// all existing pools have stale connections,
		// store refs to close them later
		stalePoolsRefs := []*connPool{}
		for _, connPool := range c.connPool {
			stalePoolsRefs = append(stalePoolsRefs, connPool)
		}

		if err == nil { // connection options update was successful
			if indexersExists {
				for _, host := range availableHosts {
					c.connPool[host] = newConnPool(host)
				}
			}

			c.availHostsMutex.Lock()
			c.availableHosts = availableHosts
			c.availHostsMutex.Unlock()
		}

		// Cleanup

		// connOpts cache cleanup
		c.connOpts.delete(removedHosts)

		// Previous Event was Indexers Existence Toggle
		// Which would have closed all the conn pools already
		if !indexersExists {
			return nil
		}

		// close stale pools
		for _, connPool := range stalePoolsRefs {
			connPool.close(getUpdateReason(true))
		}

		// remove stale pool entries
		availableHostsSet := make(map[string]struct{})
		for _, host := range availableHosts {
			availableHostsSet[host] = struct{}{}
		}
		for host, _ := range c.connPool {
			if _, ok := availableHostsSet[host]; !ok {
				delete(c.connPool, host)
			}
		}

		return nil
	}

	// ## Handle TopologyChange

	// Stop new queries from picking up conn object from removed hosts.
	if len(removedHosts) > 0 {
		c.availHostsMutex.Lock()
		c.availableHosts = cbgt.StringsRemoveStrings(c.availableHosts, removedHosts)
		c.availHostsMutex.Unlock()
	}

	// start connection pools for addedHosts

	var availableHosts []string
	var addedAvailableHosts []string
	if len(addedHosts) > 0 {
		addedAvailableHosts, err = c.connOpts.update(
			addedHosts, connType, isSecCfgChanged, secConfig)

		if err != nil {
			logging.Infof("n1fty: failed to create/update connOpts for "+
				"hosts:%+v, err: %v", addedHosts, err)
		} else {
			availableHosts = append(availableHosts, addedAvailableHosts...)
		}
	}

	// previously unavailable hosts which became available
	nowAvailableHosts := c.connOpts.updateHostsAvailability(secConfig)

	// Some of the nowAvailableHosts may be removedHosts
	nowAvailableHosts = cbgt.StringsRemoveStrings(nowAvailableHosts,
		removedHosts)

	availableHosts = append(availableHosts, nowAvailableHosts...)

	if len(availableHosts) != 0 {
		// Start connection pools for availableHosts
		if indexersExists {
			for _, host := range availableHosts {
				c.connPool[host] = newConnPool(host)
			}
		}

		// Start serving queries from availableHosts also
		c.availHostsMutex.Lock()
		c.availableHosts = append(c.availableHosts, availableHosts...)
		c.availHostsMutex.Unlock()
	}

	// Cleanup
	c.connOpts.delete(removedHosts)

	// Previous Event was Indexers Existence Toggle
	// Which would have closed all the conn pools already
	if !indexersExists {
		return nil
	}

	for _, removedHost := range removedHosts {
		if connPool, ok := c.connPool[removedHost]; ok {
			connPool.close(getUpdateReason(false))
			delete(c.connPool, removedHost)
		}
	}

	return nil
}

func (c *ftsClient) getGrpcClient() (
	pb.SearchServiceClient, *connPool, *grpc.ClientConn) {
	c.availHostsMutex.RLock()
	defer c.availHostsMutex.RUnlock()

	if len(c.availableHosts) == 0 {
		logging.Infof("n1fty: getGrpcClient, err: No Available search service host")
		return nil, nil, nil
	}

	// Pick a random available FTS node
	randomNodeIndex := rand.Intn(len(c.availableHosts))

	connPool, ok := c.connPool[c.availableHosts[randomNodeIndex]]
	if !ok {
		logging.Infof("n1fty: No connection pool for %v",
			c.availableHosts[randomNodeIndex])
		return nil, nil, nil
	}

	conn, err := connPool.get()
	if err != nil {
		logging.Infof("n1fty: Failed to Get a connection from pool:%v, "+
			"error:%v", c.availableHosts[randomNodeIndex], err)
		return nil, nil, nil
	}

	return pb.NewSearchServiceClient(conn), connPool, conn
}

func (c *ftsClient) reconcileServers(
	newServers []string) (addedServers, removedServers []string) {
	oldServers := c.servers

	oldServerSet := make(map[string]struct{})
	for _, s := range oldServers {
		oldServerSet[s] = struct{}{}
	}

	newServerSet := make(map[string]struct{})
	for _, s := range newServers {
		newServerSet[s] = struct{}{}
	}

	for _, server := range newServers {
		if _, ok := oldServerSet[server]; !ok {
			addedServers = append(addedServers, server)
		}
	}

	for _, server := range oldServers {
		if _, ok := newServerSet[server]; !ok {
			removedServers = append(removedServers, server)
		}
	}

	return
}

// -----------------------------------------------------------------------------

// concurrent safe grpc.Dialoption(s) cache
type grpcOpts struct {
	mu           sync.RWMutex
	commonOpts   []grpc.DialOption            // host agnostic
	hostOpts     map[string][]grpc.DialOption // host: {commonOpts + creds}
	unAvailHosts map[string]struct{}          // cbauth failed to get creds
}

func newGrpcOpts() *grpcOpts {
	return &grpcOpts{
		hostOpts:     make(map[string][]grpc.DialOption),
		unAvailHosts: make(map[string]struct{}),
	}
}

func (g *grpcOpts) delete(hosts []string) {
	if hosts == nil || len(hosts) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, host := range hosts {
		delete(g.hostOpts, host)
		delete(g.unAvailHosts, host)
	}
}

func (g *grpcOpts) get(host string) ([]grpc.DialOption, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	opts, ok := g.hostOpts[host]
	return opts, ok
}

// Retry reaching out to previously unavailable hosts
func (g *grpcOpts) updateHostsAvailability(
	secConfig *securityConfig) (nowAvailableHosts []string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for host := range g.unAvailHosts {
		cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(host)
		if err != nil {
			// This host is still unreachable
			continue
		}

		// mark host available
		logging.Infof("n1fty: host:%v became available", host)
		delete(g.unAvailHosts, host)
		nowAvailableHosts = append(nowAvailableHosts, host)

		// copy g.commonOpts
		g.hostOpts[host] = make([]grpc.DialOption, len(g.commonOpts))
		copy(g.hostOpts[host], g.commonOpts)

		g.hostOpts[host] = append(
			g.hostOpts[host], grpc.WithPerRPCCredentials(&basicAuthCreds{
				username:                 cbUser,
				password:                 cbPasswd,
				requireTransportSecurity: secConfig.encryptionEnabled,
			}))
	}

	return
}

const maxConnRetryAttempts = 10

// This function update(or create) grpc.DialOption(s) for given hosts
// and cache them
//
// Return list of unavailable hosts and error if any
func (g *grpcOpts) update(
	hosts []string, connType connectionType, isSecCfgChanged bool,
	secConfig *securityConfig) ([]string, error) {
	if len(hosts) == 0 {
		return nil, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	availableHosts := []string{}

	if isSecCfgChanged || len(g.commonOpts) == 0 {
		err := g.updateCommonLOCKED(connType, secConfig)
		if err != nil {
			logging.Infof("n1fty: failed to update common grpc options"+
				", err:%v", err)
			return availableHosts, err
		}
	}

	startSleepMS, maxSleepMS, backoffFactor := 50, 500, float32(1.5)
	// Around 10 attempts (maxConnRetryAttempts) in a span of ~2s
	for _, host := range hosts {
		var cbUser, cbPasswd string
		err := util.ExponentialBackoffRetry(func() error {
			var err error
			cbUser, cbPasswd, err = cbauth.GetHTTPServiceAuth(host)
			return err
		}, startSleepMS, maxSleepMS, backoffFactor, maxConnRetryAttempts)

		if err != nil {
			// it is possible that some hosts may be unreachable during
			// a cluster operation, so ignore error here; see: MB-40125
			logging.Infof("n1fty: host:%v unavailable, failed to get "+
				"credentials, err: %v", host, err)
			g.unAvailHosts[host] = struct{}{}
			continue
		}

		availableHosts = append(availableHosts, host)
		delete(g.unAvailHosts, host)

		// copy common grpc options for the host
		g.hostOpts[host] = make([]grpc.DialOption, len(g.commonOpts))
		copy(g.hostOpts[host], g.commonOpts)

		g.hostOpts[host] = append(g.hostOpts[host],
			grpc.WithPerRPCCredentials(&basicAuthCreds{
				username:                 cbUser,
				password:                 cbPasswd,
				requireTransportSecurity: secConfig.encryptionEnabled,
			}))
	}

	return availableHosts, nil
}

func (g *grpcOpts) updateCommonLOCKED(connType connectionType, secConfig *securityConfig) error {
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
	}

	if connType == sslConn {
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(secConfig.certInBytes)
		if !ok {
			return fmt.Errorf("client: failed to append ca certs")
		}
		cred := credentials.NewClientTLSFromCert(certPool, "")
		gRPCOpts = append(gRPCOpts, grpc.WithTransportCredentials(cred))
	} else if connType == nonSslConn {
		gRPCOpts = append(gRPCOpts, grpc.WithInsecure())
	} else {
		return fmt.Errorf("hosts (ssl/non-ssl) unavailable")
	}

	g.commonOpts = gRPCOpts
	return nil
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

// ------------------------------Utility Functions------------------------------

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
