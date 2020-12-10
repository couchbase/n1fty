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
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/n1fty/util"
	"gopkg.in/couchbase/gocbcore.v7"
)

var ConnectTimeoutMS = time.Duration(60000 * time.Millisecond)
var ServerConnectTimeoutMS = time.Duration(7000 * time.Millisecond)
var NmvRetryDelayMS = time.Duration(100 * time.Millisecond)

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

func setupAgent(server, bucket string) (*gocbcore.Agent, error) {
	conf := &gocbcore.AgentConfig{
		UserString:           "n1fty",
		BucketName:           bucket,
		ConnectTimeout:       ConnectTimeoutMS,
		ServerConnectTimeout: ServerConnectTimeoutMS,
		NmvRetryDelay:        NmvRetryDelayMS,
		UseKvErrorMaps:       true,
		Auth:                 &Authenticator{},
	}

	err := conf.FromConnStr(server)
	if err != nil {
		return nil, util.N1QLError(err, "")
	}

	return gocbcore.CreateAgent(conf)
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
