//  Copyright (c) 2020 Couchbase, Inc.
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
	"io/ioutil"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/query/logging"
)

var BackfillMonitoringIntervalMS = time.Duration(1000 * time.Millisecond)
var StatsLoggingIntervalMS = time.Duration(60000 * time.Millisecond)

// ----------------------------------------------------------------------------

var mr *monitor

func init() {
	mr = &monitor{
		indexers: make(map[string]*FTSIndexer),
	}

	go mr.backfillMonitor()
	go mr.logStats()
}

// ----------------------------------------------------------------------------

type monitor struct {
	m        sync.RWMutex
	indexers map[string]*FTSIndexer
}

func (m *monitor) registerIndexer(i *FTSIndexer) {
	if i != nil {
		m.m.Lock()
		m.indexers[i.BucketId()+i.ScopeId()+i.KeyspaceId()] = i
		m.m.Unlock()
	}
}

func (m *monitor) unregisterIndexer(i *FTSIndexer) {
	if i != nil {
		m.m.Lock()
		delete(mr.indexers, i.BucketId()+i.ScopeId()+i.KeyspaceId())
		m.m.Unlock()
	}
}

// ----------------------------------------------------------------------------

// Blocking method; To be spun off as a goroutine
func (m *monitor) backfillMonitor() {
	tick := time.NewTicker(BackfillMonitoringIntervalMS)
	defer tick.Stop()

	for {
		<-tick.C

		backfillDir := getBackfillSpaceDir()
		files, err := ioutil.ReadDir(backfillDir)
		if err != nil {
			logging.Warnf("n1fty backfill monitor failed to read dir,"+
				" err: %v", err)
			continue
		}

		var size int64
		for _, file := range files {
			fname := path.Join(backfillDir, file.Name())
			if strings.Contains(fname, backfillPrefix) {
				size += file.Size()
			}
		}

		m.m.RLock()
		for _, i := range m.indexers {
			atomic.StoreInt64(&i.stats.CurBackFillSize, size)
		}
		m.m.RUnlock()
	}
}

// Blocking method; To be spun off as a goroutine
func (m *monitor) logStats() {
	tick := time.NewTicker(StatsLoggingIntervalMS)
	defer tick.Stop()

	for {
		<-tick.C

		m.m.RLock()
		for _, i := range m.indexers {
			searchDur := atomic.LoadInt64(&i.stats.TotalSearchDuration)
			n1qlDur := atomic.LoadInt64(&i.stats.TotalThrottledN1QLDuration)
			ftsDur := atomic.LoadInt64(&i.stats.TotalThrottledFtsDuration)
			ttfbDur := atomic.LoadInt64(&i.stats.TotalTTFBDuration)
			totalSearch := atomic.LoadInt64(&i.stats.TotalSearch)
			totalBackfills := atomic.LoadInt64(&i.stats.TotalBackFills)

			fmsg := `n1fty bucket-scope-keyspace: %q.%q.%q {` +
				`"n1fty_search_count":%v,"n1fty_search_duration":%v,` +
				`"n1fty_fts_duration":%v,` +
				`"n1fty_ttfb_duration":%v,"n1fty_n1ql_duration":%v,` +
				`"n1fty_totalbackfills":%v}`
			logging.Infof(fmsg,
				i.BucketId(), i.ScopeId(), i.KeyspaceId(), totalSearch,
				searchDur, ftsDur, ttfbDur, n1qlDur, totalBackfills)
		}
		m.m.RUnlock()

	}
}
