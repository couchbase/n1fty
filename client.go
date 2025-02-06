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
	"github.com/couchbase/query/logging"

	pb "github.com/couchbase/cbft/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// CBAUTH security/encryption config
type securityConfig struct {
	encryptionEnabled          bool
	disableNonSSLPorts         bool
	shouldClientsUseClientCert bool
	certificate                *tls.Certificate
	clientAuthType             *tls.ClientAuthType
	clientCertificate          *tls.Certificate
	certInBytes                []byte
	tlsPreference              *cbauth.TLSConfig
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

type ftsClient struct {
	m sync.RWMutex // Protects the fields that follow.

	// cache the list of host:port info for all the fts nodes in the cluster.
	// Useful for reconcilation during topology change.
	hosts []string

	sslHostsAvailable    bool // are there any grpc-ssl fts hosts in the cluster?
	nonSslHostsAvailable bool // are there any grpc-non-ssl fts hosts in the cluster?

	// cache the grpc.DialOption(s) for all the servers
	// access to methods of grpcOpts is protected by ftsClient.m
	connOpts *grpcOpts

	// Depends on sslHostsAvailable, nonSslHostsAvailable and the security config
	// params (like: encryptionEnabled, disabledNonSSLPorts)
	//
	// connType is __nilConnType when there are no fts nodes in the cluster.
	// or, fts nodes are not able to satisfy the security config params requirement.
	//
	// Updated on TopologyChange and SecurityConfigChange events
	connType connectionType

	// A connPool per fts node
	connPool map[string]*connPool
}

type connectionType int

const (
	sslConn connectionType = iota
	nonSslConn
	__nilConnType
)

type connPoolUpdateEvent int

const (
	// includes changes to cluster encryption settings and/or certs tls config
	// settings
	securityConfigChange connPoolUpdateEvent = iota
	// tied to cbgt.Cfg notification corresponding to NodeDefsKnown key.
	// includes addition/removal of fts nodes in the cluster
	topologyChange
	// signalled when indexers count goes from 0 to 1 and vice-versa
	indexerAvailabilityChange
)

// event tags
var eventTags = map[connPoolUpdateEvent]string{
	securityConfigChange:      "SecurityConfigChange",
	topologyChange:            "TopologyChange",
	indexerAvailabilityChange: "IndexerAvailabilityChange",
}

// Instance of ftsClient
var ftsClientInst *ftsClient

func init() {
	rand.Seed(time.Now().UnixNano())

	ftsClientInst = &ftsClient{
		connPool: make(map[string]*connPool),
		connType: __nilConnType,
	}
	ftsClientInst.connOpts = newGrpcOpts(ftsClientInst)
}

func (c *ftsClient) resetLOCKED() {
	c.hosts = nil
	c.sslHostsAvailable = false
	c.nonSslHostsAvailable = false
	c.connType = __nilConnType
	c.connOpts.reset()

	// close all conn pools
	for host, _ := range c.connPool {
		c.connPool[host].close("Reset")
		delete(c.connPool, host)
	}
}

func (c *ftsClient) updateConnTypeLOCKED() {
	secConfig := loadSecurityConfig()
	if secConfig.encryptionEnabled && c.sslHostsAvailable {
		c.connType = sslConn
	} else if !secConfig.disableNonSSLPorts && c.nonSslHostsAvailable {
		c.connType = nonSslConn
	} else {
		logging.Errorf("n1fty: hosts unable to satisfy the security config "+
			"{encryptionEnabled: %v, disableNonSSLPorts: %v}",
			secConfig.encryptionEnabled, secConfig.disableNonSSLPorts)
		c.connType = __nilConnType
	}
}

// useful for updating connection options and/or creating new connection pools
func (c *ftsClient) createConnPoolsLOCKED(hosts []string,
	updateConnOpts, createPools bool) error {
	if updateConnOpts {
		// create/update dial options for hosts
		err := c.connOpts.updateHostOpts(hosts)
		if err != nil {
			c.resetLOCKED()
			return fmt.Errorf("failed to update grpc opts for hosts, err: %v", err)
		}
	}

	if createPools {
		for _, host := range hosts {
			c.connPool[host] = newConnPool(host)
		}
	}

	return nil
}

// useful for cleaning up connection options and/or closing connection pools
func (c *ftsClient) closeConnPoolsLOCKED(hosts []string, reason string,
	cleanupConnOpts bool, deletePools bool) {
	if cleanupConnOpts {
		c.connOpts.delete(hosts)
	}

	if deletePools {
		for _, host := range hosts {
			if connPool, ok := c.connPool[host]; ok {
				connPool.close(reason)
				delete(c.connPool, host)
			}
		}
	}
}

func (c *ftsClient) updateConnPools(event connPoolUpdateEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.updateConnPoolsLOCKED(event)
}

func (c *ftsClient) updateConnPoolsLOCKED(event connPoolUpdateEvent) error {
	logging.Infof("n1fty: updateConnPools start, event:%v", eventTags[event])
	defer func() {
		logging.Infof("n1fty: updateConnPools done, hosts:%v", c.hosts)
	}()

	switch event {
	case securityConfigChange:
		return c.onSecurityConfigChange()
	case topologyChange:
		return c.onTopologyChange()
	case indexerAvailabilityChange:
		c.onIndexerAvailabilityChange()
	default:
		return fmt.Errorf("unknown event")
	}

	return nil
}

func (c *ftsClient) onSecurityConfigChange() error {
	indexerAvail := indexersExist()
	logging.Infof("n1fty: onSecurityConfigChange, indexerAvail:%v", indexerAvail)

	if len(c.hosts) == 0 {
		return nil
	}

	c.updateConnTypeLOCKED()
	if c.connType == __nilConnType {
		c.resetLOCKED()
		return fmt.Errorf("nil connection type")
	}

	err := c.connOpts.updateCommonOpts()
	if err != nil {
		return fmt.Errorf("failed to update common grpc options, err: %v", err)
	}

	// # Update Connection Pools
	// SecurityConfigChange is involved in the book-keeping of connection options.
	// Whether to materialize conn pool creation/deletion is decided by the
	// indexer availability.
	//
	// Close existing stale connection pools and create new ones.
	c.closeConnPoolsLOCKED(c.hosts, eventTags[securityConfigChange], true, indexerAvail)
	if err := c.createConnPoolsLOCKED(c.hosts, true, indexerAvail); err != nil {
		c.resetLOCKED()
		return fmt.Errorf("failed to create connection pools, err: %v", err)
	}

	return nil
}

func (c *ftsClient) onTopologyChange() error {
	indexerAvail := indexersExist()
	logging.Infof("n1fty: onTopologyChange, indexerAvail:%v", indexerAvail)

	// ## Fetch new topology and reconcile with existing topology

	nodeDefs, err := GetNodeDefs(srvConfig)
	if err != nil {
		c.resetLOCKED()
		return fmt.Errorf("GetNodeDefs failed, err: %v", err)
	}

	if nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		c.resetLOCKED()
		return fmt.Errorf("GetNodeDefs, err: hosts unavailable")
	}

	nonSslHosts, sslHosts := extractHosts(nodeDefs)

	// no fts nodes in the cluster support grpc
	if len(nonSslHosts) == 0 && len(sslHosts) == 0 {
		c.resetLOCKED()
		return ErrFeatureUnavailable
	}

	c.sslHostsAvailable = len(sslHosts) != 0
	c.nonSslHostsAvailable = len(nonSslHosts) != 0

	c.updateConnTypeLOCKED()
	if c.connType == __nilConnType {
		c.resetLOCKED()
		return fmt.Errorf("nil connection type")
	}

	var newHosts []string
	if c.connType == sslConn {
		newHosts = sslHosts
	} else {
		newHosts = nonSslHosts
	}

	// reconcile topology
	keepHosts := cbgt.StringsIntersectStrings(c.hosts, newHosts)
	removedHosts := cbgt.StringsRemoveStrings(c.hosts, newHosts)
	addedHosts := cbgt.StringsRemoveStrings(newHosts, keepHosts)
	logging.Infof("n1fty: onTopologyChange, keepHosts:%v, removedHosts:%v, "+
		"addedHosts:%v", keepHosts, removedHosts, addedHosts)

	// update hosts
	c.hosts = newHosts

	// # Update Conn Pools
	// Topology Change is involved in the book-keeping of connection options.
	// Whether to materialize conn pool creation/deletion is decided by the
	// indexer availability.
	//
	// close pools corresponding to removed hosts and create new ones for added hosts.
	c.closeConnPoolsLOCKED(removedHosts, eventTags[topologyChange], true,
		indexerAvail)
	if err := c.createConnPoolsLOCKED(addedHosts, true, indexerAvail); err != nil {
		c.resetLOCKED()
		return fmt.Errorf("failed to create connection pools, err: %v", err)
	}

	return nil
}

// Handler for indexer availability change event.
// The mutex n1fty.monitor.m is expected to be locked for the entire duration of this
// function execution. This is to handle multiple concurrent indexer availability
// change events.
func (c *ftsClient) onIndexerAvailabilityChange() {
	indexerAvail := indexersExist()
	logging.Infof("n1fty: onIndexerAvailabilityChange, indexer available:%v",
		indexerAvail)

	// # Update Conn Pools
	// We don't need to update the connection options here, as book-keeping of it
	// is already done in onTopologyChange and onSecurityConfigChange.
	//
	// On IndexerAvailabilityChange, we bring-up/tear-down the connection pools.
	if indexerAvail { // indexers count went from 0 -> 1
		c.createConnPoolsLOCKED(c.hosts, false, true)
	} else { // indexers count goes went 1 -> 0
		c.closeConnPoolsLOCKED(c.hosts, eventTags[indexerAvailabilityChange],
			false, true)
	}
}

func (c *ftsClient) getGrpcClient() (
	pb.SearchServiceClient, *connPool, *grpc.ClientConn) {
	c.m.RLock()
	defer c.m.RUnlock()

	if len(c.hosts) == 0 {
		logging.Errorf("n1fty: getGrpcClient, err: search host unavailable")
		return nil, nil, nil
	}

	// randomly pick a search host
	hostIdx := rand.Intn(len(c.hosts))

	connPool, ok := c.connPool[c.hosts[hostIdx]]
	if !ok {
		logging.Errorf("n1fty: getGrpcClient, err: no connection pool exist for %v",
			c.hosts[hostIdx])
		return nil, nil, nil
	}

	conn, err := connPool.get()
	if err != nil {
		logging.Errorf("n1fty: getGrpcClient, err: failed to get a connection "+
			"object from pool:%v, error:%v", c.hosts[hostIdx], err)
		return nil, nil, nil
	}

	return pb.NewSearchServiceClient(conn), connPool, conn
}

// -----------------------------------------------------------------------------

// for concurrent access use under client.m lock.
type grpcOpts struct {
	client *ftsClient

	// common dial options for all the hosts
	commonOpts []grpc.DialOption

	// commonOpts + host specific options
	hostOpts map[string][]grpc.DialOption
}

func newGrpcOpts(client *ftsClient) *grpcOpts {
	return &grpcOpts{
		hostOpts: make(map[string][]grpc.DialOption),
		client:   client,
	}
}

func (g *grpcOpts) reset() {
	g.commonOpts = nil
	g.hostOpts = make(map[string][]grpc.DialOption)
}

func (g *grpcOpts) delete(hosts []string) {
	if hosts == nil || len(hosts) == 0 {
		return
	}

	for _, host := range hosts {
		delete(g.hostOpts, host)
	}
}

func (g *grpcOpts) get(host string) ([]grpc.DialOption, bool) {
	opts, ok := g.hostOpts[host]
	return opts, ok
}

// update dial options common to all the hosts.
// handles security config changes.
func (g *grpcOpts) updateCommonOpts() error {
	secConfig := loadSecurityConfig()

	connType := g.client.connType
	if connType == __nilConnType {
		return fmt.Errorf("nil connection type")
	}

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

	if g.client.connType == sslConn {
		certPool := x509.NewCertPool()
		ok := certPool.AppendCertsFromPEM(secConfig.certInBytes)
		if !ok {
			return fmt.Errorf("client: failed to append ca certs")
		}
		var cred credentials.TransportCredentials
		if secConfig.shouldClientsUseClientCert {
			cred = credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{*secConfig.clientCertificate},
				ClientAuth:   *secConfig.clientAuthType,
				RootCAs:      certPool,
			})
		} else {
			cred = credentials.NewClientTLSFromCert(certPool, "")
		}
		gRPCOpts = append(gRPCOpts, grpc.WithTransportCredentials(cred))
	} else { // non-ssl
		gRPCOpts = append(gRPCOpts, grpc.WithInsecure())
	}

	g.commonOpts = gRPCOpts
	return nil
}

// This function update(or create) grpc.DialOption(s) for given hosts
// and cache them
func (g *grpcOpts) updateHostOpts(hosts []string) error {
	if len(hosts) == 0 {
		return nil
	}

	if g.client.connType == __nilConnType {
		return fmt.Errorf("nil connection type")
	}

	if len(g.commonOpts) == 0 {
		err := g.updateCommonOpts()
		if err != nil {
			return fmt.Errorf("failed to update common grpc options, err: %v", err)
		}
	}

	secConfig := loadSecurityConfig()

	for _, host := range hosts {
		g.hostOpts[host] = make([]grpc.DialOption, len(g.commonOpts))
		copy(g.hostOpts[host], g.commonOpts)

		g.hostOpts[host] = append(g.hostOpts[host],
			grpc.WithPerRPCCredentials(&basicAuthCreds{
				host:                     host,
				requireTransportSecurity: secConfig.encryptionEnabled,
			}))
	}

	return nil
}

// -----------------------------------------------------------------------------

// basicAuthCreds is an implementation of credentials.PerRPCCredentials
// that transforms the username and password into a base64 encoded value
// similar to HTTP Basic xxx
type basicAuthCreds struct {
	host                     string // host:port
	requireTransportSecurity bool
}

// GetRequestMetadata sets the value for "authorization" key
func (b *basicAuthCreds) GetRequestMetadata(context.Context, ...string) (
	map[string]string, error) {
	username, password, err := cbauth.GetHTTPServiceAuth(b.host)
	if err != nil {
		logging.Warnf("n1fty: GetRequestMetadata, err: %v", err)
		return nil, err
	}

	return map[string]string{
		"authorization": "Basic " + basicAuth(username, password),
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
	nonSslHosts := []string{}
	sslHosts := []string{}

	for _, v := range nodeDefs.NodeDefs {
		var grpcFeatureSupport bool
		extrasBindGRPC, err := v.GetFromParsedExtras("bindGRPC")
		if err == nil && extrasBindGRPC != nil {
			if bindGRPCstr, ok := extrasBindGRPC.(string); ok {
				if bindGRPCstr != "" {
					nonSslHosts = append(nonSslHosts, bindGRPCstr)
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

	return nonSslHosts, sslHosts
}

func indexersExist() bool {
	numIndexers := mr.indexerCount()
	return numIndexers != 0
}

func getTopologyAndConnType() ([]string, connectionType, error) {
	nodeDefs, err := GetNodeDefs(srvConfig)
	if err != nil {
		logging.Errorf("n1fty: GetNodeDefs, err: %v", err)
		return nil, __nilConnType, fmt.Errorf("GetNodeDefs failed [err: %v]", err)
	}

	if nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		return nil, __nilConnType, fmt.Errorf("hosts (ssl/non-ssl) unavailable")
	}

	hosts, sslHosts := extractHosts(nodeDefs)
	if len(hosts) == 0 && len(sslHosts) == 0 {
		return nil, __nilConnType, ErrFeatureUnavailable
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
		return nil, __nilConnType, fmt.Errorf("hosts (ssl/non-ssl) unavailable")
	}
	return newTopology, connType, nil
}
