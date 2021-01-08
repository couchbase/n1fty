// Copyright (c) 2020 Couchbase, Inc.
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
	"crypto/tls"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcore/v9"
)

var ConnectTimeoutMS = time.Duration(60000 * time.Millisecond)
var KVConnectTimeoutMS = time.Duration(7000 * time.Millisecond)
var AgentSetupTimeoutMS = time.Duration(10000 * time.Millisecond)

// -----------------------------------------------------------------------------

type agentDetails struct {
	refs  int
	agent *gocbcore.Agent
}

type gocbcoreAgentMap struct {
	// mutex to serialize access to entries
	m sync.Mutex
	// map of gocbcore.Agent instances by bucket <name>:<uuid>
	entries map[string]*agentDetails
}

var agentMap *gocbcoreAgentMap

func init() {
	agentMap = &gocbcoreAgentMap{
		entries: make(map[string]*agentDetails),
	}
}

// -----------------------------------------------------------------------------

func (am *gocbcoreAgentMap) acquireAgent(server, bucket string) (
	*gocbcore.Agent, error) {
	am.m.Lock()
	defer am.m.Unlock()

	if _, exists := am.entries[bucket]; !exists {
		agent, err := setupAgent(server, bucket)
		if err != nil {
			return nil, err
		}

		am.entries[bucket] = &agentDetails{
			agent: agent,
		}
	}

	am.entries[bucket].refs++
	return am.entries[bucket].agent, nil
}

func (am *gocbcoreAgentMap) releaseAgent(bucket string) {
	am.m.Lock()
	defer am.m.Unlock()

	if _, exists := am.entries[bucket]; exists {
		am.entries[bucket].refs--
		if am.entries[bucket].refs > 0 {
			return
		}

		go am.entries[bucket].agent.Close()
		delete(am.entries, bucket)
	}
}

// -----------------------------------------------------------------------------

type retryStrategy struct{}

func (rs *retryStrategy) RetryAfter(req gocbcore.RetryRequest,
	reason gocbcore.RetryReason) gocbcore.RetryAction {
	if reason == gocbcore.BucketNotReadyReason {
		return &gocbcore.WithDurationRetryAction{
			WithDuration: gocbcore.ControlledBackoff(req.RetryAttempts()),
		}
	}

	return &gocbcore.NoRetryRetryAction{}
}

func setupAgent(server, bucket string) (*gocbcore.Agent, error) {
	conf := &gocbcore.AgentConfig{
		UserAgent:        "n1fty",
		BucketName:       bucket,
		ConnectTimeout:   ConnectTimeoutMS,
		KVConnectTimeout: KVConnectTimeoutMS,
		Auth:             &Authenticator{},
	}

	connStr := server
	if connURL, err := url.Parse(server); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}

	err := conf.FromConnStr(connStr)
	if err != nil {
		return nil, err
	}

	agent, err := gocbcore.CreateAgent(conf)
	if err != nil {
		return nil, err
	}

	options := gocbcore.WaitUntilReadyOptions{
		DesiredState:  gocbcore.ClusterStateOnline,
		ServiceTypes:  []gocbcore.ServiceType{gocbcore.MemdService},
		RetryStrategy: &retryStrategy{},
	}

	signal := make(chan error, 1)
	_, err = agent.WaitUntilReady(time.Now().Add(AgentSetupTimeoutMS),
		options, func(res *gocbcore.WaitUntilReadyResult, er error) {
			signal <- er
		})

	if err == nil {
		err = <-signal
	}

	if err != nil {
		go agent.Close()
		return nil, err
	}

	return agent, nil
}

type Authenticator struct{}

func (a *Authenticator) Credentials(req gocbcore.AuthCredsRequest) (
	[]gocbcore.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(
		endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocbcore.UserPassPair{{}}, err
	}

	return []gocbcore.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func (a *Authenticator) Certificate(req gocbcore.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *Authenticator) SupportsTLS() bool {
	return true
}

func (a *Authenticator) SupportsNonTLS() bool {
	return true
}
