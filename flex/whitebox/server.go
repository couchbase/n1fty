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
	"encoding/json"
	net_http "net/http"
	"time"

	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/file"
	"github.com/couchbase/query/datastore/system"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/execution"
	"github.com/couchbase/query/logging"
	log_resolver "github.com/couchbase/query/logging/resolver"
	"github.com/couchbase/query/server"
	"github.com/couchbase/query/server/http"
	"github.com/couchbase/query/timestamp"
	"github.com/couchbase/query/value"
)

func init() {
	logger, _ := log_resolver.NewLogger("golog")
	logging.SetLogger(logger)
}

// Initialize a new server for a file datastore given a directory
// path.
func NewServer(dir string, c *WrapCallbacks) (*server.Server, error) {
	ds, err := file.NewDatastore(dir)
	if err != nil {
		return nil, err
	}

	wds := &WrapDatastore{W: ds, C: c}

	sys, err := system.NewDatastore(wds)
	if err != nil {
		return nil, err
	}

	return server.NewServer(wds, sys, nil, nil, "json",
		false, 10, 10, 4, 4, 0, 0, false, false, false, true,
		server.ProfOff, false)
}

// ------------------------------------------------------------

type Request struct {
	server.BaseRequest

	err     errors.Error
	done    chan bool
	results []interface{}
}

func (this *Request) OriginalHttpRequest() *net_http.Request {
	return nil
}

func (this *Request) Output() execution.Output {
	return this
}

func (this *Request) Execute(s *server.Server, context *execution.Context, reqType string, signature value.Value, b bool) {
	select {
	case <-this.Results():
	case <-this.StopExecute():
	}
	close(this.done)
}

func (this *Request) Fail(err errors.Error) {
	defer this.Stop(server.FATAL)
	this.err = err
	close(this.done)
}

func (this *Request) Failed(s *server.Server) {
}

func (this *Request) Expire(state server.State, timeout time.Duration) {
	defer this.Stop(state)
	this.err = util.N1QLError(nil, "expired / timed out")
	close(this.done)
}

func (this *Request) SetUp() {
}

func (this *Request) ScanConsistency() datastore.ScanConsistency {
	return datastore.SCAN_PLUS
}

func (this *Request) ScanWait() time.Duration {
	return 0
}

func (this *Request) ScanVectorSource() timestamp.ScanVectorSource {
	return &http.ZeroScanVectorSource{}
}

func (this *Request) Result(item value.AnnotatedValue) bool {
	bytes, err := json.Marshal(item)
	if err != nil {
		this.SetState(server.FATAL)
		panic(err.Error())
	}

	var result map[string]interface{}

	_ = json.Unmarshal(bytes, &result)

	this.results = append(this.results, result)

	return true
}

// ------------------------------------------------------------

func ExecuteStatement(s *server.Server, stmt string,
	namedArgs map[string]value.Value,
	positionalArgs []value.Value) (
	[]interface{}, errors.Error) {
	req := &Request{
		done: make(chan bool),
	}

	server.NewBaseRequest(&req.BaseRequest)

	req.SetStatement(stmt)
	req.SetNamedArgs(namedArgs)
	req.SetPositionalArgs(positionalArgs)

	if !s.ServiceRequest(req) {
		return nil, util.N1QLError(nil, "ServiceRequest did not work")
	}

	for range req.done {
	}

	return req.results, req.err
}
