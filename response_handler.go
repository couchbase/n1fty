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
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/search"

	pb "github.com/couchbase/cbft/protobuf"

	"github.com/couchbase/n1fty/util"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/logging"
	"github.com/couchbase/query/value"
)

var defaultBatchSize = 100 // TODO: configurability.
var defaultSizeInMB = float64(1024 * 1024)

type responseHandler struct {
	i            *FTSIndex
	requestID    string
	backfillFile *os.File
	searchReq    *pb.SearchRequest
}

func (r *responseHandler) handleResponse(conn *datastore.IndexConnection,
	waitGroup *sync.WaitGroup,
	backfillSync *int64,
	stream pb.SearchService_SearchClient) {
	sender := conn.Sender()

	backfillLimit := getBackfillSpaceLimit()

	firstResponseByte, starttm, ftsDur := false, time.Now(), time.Now()

	var enc *gob.Encoder
	var dec *gob.Decoder
	var readfd *os.File

	logPrefix := fmt.Sprintf("n1fty[%s/%s-%v]", r.i.Name(), r.i.KeyspaceId(), time.Now().UnixNano())

	var tmpfile *os.File
	var backfillFin, backfillEntries int64
	hits := make([]*search.DocumentMatch, defaultBatchSize)

	backfill := func() {
		name := tmpfile.Name()

		defer func() {
			if readfd != nil {
				readfd.Close()
			}

			waitGroup.Done()

			atomic.AddInt64(&backfillFin, 1)

			logging.Infof("response_handler: %v %q finished backfill for %v ",
				logPrefix, r.requestID, name)

			// TODO: revisit this for better pattern?
			recover() // need this because entryChannel() would have closed
		}()

		logging.Infof("response_handler: %v %q started backfill for %v",
			logPrefix, r.requestID, name)

		for {
			if pending := atomic.LoadInt64(&backfillEntries); pending > 0 {
				atomic.AddInt64(&backfillEntries, -1)
			} else if done := atomic.LoadInt64(backfillSync); done == doneRequest {
				return
			} else {
				// wait a bit
				time.Sleep(1 * time.Millisecond)
				continue
			}

			cummsize := float64(atomic.LoadInt64(
				&r.i.indexer.stats.CurBackFillSize)) / defaultSizeInMB
			if cummsize > float64(backfillLimit) {
				fmsg := "%q backfill size: %d exceeded limit: %d"
				err := fmt.Errorf(fmsg, r.requestID, cummsize, backfillLimit)
				conn.Error(util.N1QLError(err, ""))
				return
			}

			if err := dec.Decode(&hits); err != nil {
				fmsg := "%v %q decoding from backfill file: %v: err: %v"
				err = fmt.Errorf(fmsg, logPrefix, r.requestID, name, err)
				conn.Error(util.N1QLError(err, ""))
				return
			}

			atomic.AddInt64(&r.i.indexer.stats.TotalThrottledFtsDuration,
				int64(time.Since(ftsDur)))

			for i := range hits {
				connOk := r.sendEntry(hits[i], conn)
				if !connOk {
					return
				}
			}

			// reset the time taken by fts
			ftsDur = time.Now()
		}
	}

	for {
		results, err := stream.Recv()
		if err == io.EOF {
			// return as it read all data
			return
		}

		if err != nil {
			conn.Error(util.N1QLError(err, "response_handler: stream.Recv, err "))
			return
		}

		// account the time to first byte response from fts
		if firstResponseByte == false {
			atomic.AddInt64(&r.i.indexer.stats.TotalTTFBDuration,
				int64(time.Since(starttm)))
			firstResponseByte = true
		}

		var hits []*search.DocumentMatch
		switch r := results.PayLoad.(type) {
		case *pb.StreamSearchResults_Hits:
			err = json.Unmarshal(r.Hits.Bytes, &hits)
			if err != nil {
				logging.Infof("response_handler: json.Unmarshal, err: %v", err)
				continue
			}

		case *pb.StreamSearchResults_Results:
			if r.Results.Hits != nil {
				err = json.Unmarshal(r.Results.Hits, &hits)
				if err != nil {
					logging.Infof("response_handler: json.Unmarshal, err: %v", err)
					continue
				}
			}
		}

		ln := sender.Length()
		cp := sender.Capacity()

		if backfillLimit > 0 && tmpfile == nil &&
			((cp - ln) < len(hits)) {
			logging.Infof("response_handler: buffer outflow observed, cap %d len %d", cp, ln)
			enc, dec, tmpfile, err = initBackFill(logPrefix, r.requestID, r)
			if err != nil {
				conn.Error(util.N1QLError(err, "initBackFill failed, err:"))
				return
			}
			waitGroup.Add(1)
			go backfill()
		}

		// slow reader found and hence start dumping the results to the backfill file
		if tmpfile != nil {
			// whether temp-file is exhausted the limit.
			cummsize := float64(atomic.LoadInt64(
				&r.i.indexer.stats.CurBackFillSize)) / defaultSizeInMB
			if cummsize > float64(backfillLimit) {
				fmsg := "%q backfill exceeded limit %v, %v"
				err := fmt.Errorf(fmsg, r.requestID, backfillLimit, cummsize)
				conn.Error(util.N1QLError(err, ""))
				return
			}

			if atomic.LoadInt64(&backfillFin) > 0 {
				return
			}

			err := writeToBackfill(hits, enc)
			if err != nil {
				conn.Error(util.N1QLError(err, "writeToBackfill err:"))
				return
			}

			atomic.AddInt64(&backfillEntries, 1)

		} else if hits != nil {

			atomic.AddInt64(&r.i.indexer.stats.TotalThrottledFtsDuration,
				int64(time.Since(ftsDur)))

			for i := range hits {
				connOk := r.sendEntry(hits[i], conn)
				if !connOk {
					return
				}
			}

			// reset the time taken by fts
			ftsDur = time.Now()
		}
	}
}

func (r *responseHandler) cleanupBackfill() {
	if r.backfillFile != nil {
		r.backfillFile.Close()
		fname := r.backfillFile.Name()
		if err := os.Remove(fname); err != nil {
			fmsg := "%v remove backfill file %v unexpected failure: %v\n"
			logging.Errorf("response_handler: cleanupBackfills, err: %v",
				fmt.Errorf(fmsg, fname, err))
		}
		atomic.AddInt64(&r.i.indexer.stats.TotalBackFills, 1)
	}
}

func (r *responseHandler) sendEntry(hit *search.DocumentMatch,
	conn *datastore.IndexConnection) bool {
	var start time.Time
	blockedtm, blocked := int64(0), false

	sender := conn.Sender()

	cp, ln := sender.Capacity(), sender.Length()
	if ln == cp {
		start, blocked = time.Now(), true
	}

	rv := make(map[string]interface{}, 1)
	rv["score"] = hit.Score
	if r.searchReq.IncludeLocations {
		rv["locations"] = hit.Locations
	}
	if len(r.searchReq.Fields) > 0 {
		rv["fields"] = hit.Fields
	}
	if r.searchReq.Explain {
		rv["explanation"] = *hit.Expl
	}

	if !sender.SendEntry(&datastore.IndexEntry{PrimaryKey: hit.ID,
		MetaData: value.NewValue(rv)}) {
		return false
	}

	if blocked {
		blockedtm += int64(time.Since(start))
		atomic.AddInt64(&r.i.indexer.stats.TotalThrottledN1QLDuration, blockedtm)
	}

	return true
}

func logStats(logtick time.Duration, i *FTSIndexer) {
	tick := time.NewTicker(logtick)
	defer tick.Stop()

	var sofar int64
	for {
		select {
		case <-i.closeCh:
			return

		case <-tick.C:
			searchDur := atomic.LoadInt64(&i.stats.TotalSearchDuration)
			n1qlDur := atomic.LoadInt64(&i.stats.TotalThrottledN1QLDuration)
			ftsDur := atomic.LoadInt64(&i.stats.TotalThrottledFtsDuration)
			ttfbDur := atomic.LoadInt64(&i.stats.TotalTTFBDuration)
			totalSearch := atomic.LoadInt64(&i.stats.TotalSearch)
			totalBackfills := atomic.LoadInt64(&i.stats.TotalBackFills)

			if totalSearch > sofar {
				fmsg := `n1fty keyspace: %q {` +
					`"n1fty_search_count":%v,"n1fty_search_duration":%v,` +
					`"n1fty_fts_duration":%v,` +
					`"n1fty_ttfb_duration":%v,"n1fty_n1ql_duration":%v,` +
					`"n1fty_totalbackfills":%v}`
				logging.Infof(
					fmsg, i.keyspace, totalSearch, searchDur,
					ftsDur, ttfbDur, n1qlDur, totalBackfills)
			}
			sofar = totalSearch
		}
	}
}

func backfillMonitor(period time.Duration, i *FTSIndexer) {
	tick := time.NewTicker(period)
	defer tick.Stop()

	for {
		select {
		case <-i.closeCh:
			return

		case <-tick.C:
			backfillDir := getBackfillSpaceDir()

			files, err := ioutil.ReadDir(backfillDir)
			if err != nil {
				return
			}

			var size int64
			for _, file := range files {
				fname := path.Join(backfillDir, file.Name())
				if strings.Contains(fname, backfillPrefix) {
					size += file.Size()
				}
			}

			atomic.StoreInt64(&i.stats.CurBackFillSize, size)
		}
	}
}

// TODO: need to cleanup any orphaned backfill subdirs from last time
// if there was a process crash and restart?

func initBackFill(logPrefix, requestID string, rh *responseHandler) (*gob.Encoder,
	*gob.Decoder, *os.File, error) {
	prefix := backfillPrefix + strconv.Itoa(os.Getpid())

	tmpfile, err := ioutil.TempFile(getBackfillSpaceDir(), prefix)
	if err != nil {
		fmsg := "%v %s creating backfill file, err: %v\n"
		return nil, nil, nil, fmt.Errorf(fmsg, logPrefix, requestID, err)
	}

	name := ""
	if tmpfile != nil {
		name = tmpfile.Name()
		rh.backfillFile = tmpfile
	}

	// encoder
	enc := gob.NewEncoder(tmpfile)
	readfd, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		fmsg := "%v %v reading backfill file %v, err: %v\n"
		return nil, nil, tmpfile, fmt.Errorf(fmsg, logPrefix, requestID, name, err)
	}

	// decoder
	return enc, gob.NewDecoder(readfd), tmpfile, nil
}

func writeToBackfill(hits []*search.DocumentMatch, enc *gob.Encoder) error {
	if err := enc.Encode(hits); err != nil {
		return err
	}
	return nil
}
