//  Copyright (c) 2018 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/query/datastore"
	qerrors "github.com/couchbase/query/errors"
	qexpr "github.com/couchbase/query/expression"
	qparser "github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

func clusterAuthUrl(cluster string) (string, error) {

	if strings.HasPrefix(cluster, "http") {
		u, err := url.Parse(cluster)
		if err != nil {
			return "", err
		}
		cluster = u.Host
	}

	/*adminUser, adminPasswd, err := cbauth.GetHTTPServiceAuth(cluster)
	if err != nil {
		return "", err
	}*/

	adminUser := "Administrator"
	adminPasswd := "asdasd"

	clusterUrl := url.URL{
		Scheme: "http",
		Host:   cluster,
		User:   url.UserPassword(adminUser, adminPasswd),
	}

	return clusterUrl.String(), nil
}

func TestFTSBufferedScan_BackfillEnabled(t *testing.T) {
	log.Printf("In TestFTSBufferedScan_BackfillEnabled()")

	var indexName = "FTS"
	var clusterAddress = "http://localhost:9000"

	cluster, err := clusterAuthUrl(clusterAddress)
	if err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed in getting ClusterAuthUrl", err)
	}
	log.Printf("cluster %s", cluster)

	ftsClient, err := NewFTSIndexer(cluster, "default" /*namespace*/, "travel-sample")
	if err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed in creating n1fty client", err)
	}

	rangeKey := []string{`address`}
	rangeExprs := make(qexpr.Expressions, 0)
	for _, key := range rangeKey {
		expr, err := qparser.Parse(key)
		if err != nil {
			err = fmt.Errorf("CompileN1QLExpression() %v: %v\n", key, err)
			t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", err)
		}
		rangeExprs = append(rangeExprs, expr)
	}

	in := "__FTS__/" + indexName + "/city"
	index, err := ftsClient.IndexByName(in)
	if err != nil {
		//t.Fatalf("%v: %v\n", "estFTSBufferedScan_BackfillEnabled failed in getting IndexByName", err)
	}

	// query setup
	low := value.Values{value.NewValue("London")}
	high := value.Values{value.NewValue("London")}
	rng := datastore.Range{Low: low, High: high, Inclusion: datastore.BOTH}
	span := &datastore.Span{Seek: nil, Range: rng}
	doquery := func(limit int64, conn *datastore.IndexConnection) {
		index.Scan(
			"bufferedscan", /*requestId*/
			span,
			false, /*distinct*/
			limit,
			datastore.UNBOUNDED,
			nil,
			conn,
		)
	}

	// ******* Case 1
	// no file should be created and we get valid results
	// cap(ch) == 200 & limit 10, read slow
	cleanbackfillFiles()
	ctxt := &qcmdContext{}
	conn, err := datastore.NewSizedIndexConnection(200, ctxt)
	if err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", err)
	}
	count, ch := 0, conn.EntryChannel()
	now := time.Now()
	go doquery(int64(10), conn)

	if len(getbackfillFiles(backfillDir())) != 0 {
		e := errors.New("Expected no backfill file at start")
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", e)
	}

	for range ch {
		time.Sleep(1 * time.Millisecond) // slow read
		count++
	}

	log.Printf("limit=10,chsize=256; received %v items; took %v\n",
		count, time.Since(now))

	if len(getbackfillFiles(backfillDir())) > 0 {
		e := errors.New("Unexpected backfill file")
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", e)
	} else if ctxt.err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", ctxt.err)
	}

	// ******* Case 2
	// no file should be created and we get valid results
	// cap(ch) == 100 & limit 1000, read fast
	log.Printf("new test\n\n")
	cleanbackfillFiles()
	ctxt = &qcmdContext{}
	conn, err = datastore.NewSizedIndexConnection(100, ctxt)
	if err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", err)
	}
	count, ch = 0, conn.EntryChannel()
	now = time.Now()
	go doquery(int64(1000), conn)

	for range ch {
		count++
	}

	log.Printf("limit=1000,chsize=100; received %v items; took %v\n",
		count, time.Since(now))

	if len(getbackfillFiles(backfillDir())) > 0 {
		e := errors.New("Unexpected no backfill file")
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", e)
	} else if ctxt.err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", ctxt.err)
	}

	// ******* Case 3
	// cap(ch) == 10 & limit 1000 & read slow,
	// file should be created and we get valid results and file is deleted.
	log.Printf("new test\n\n")
	cleanbackfillFiles()
	ctxt = &qcmdContext{}
	conn, err = datastore.NewSizedIndexConnection(10, ctxt)
	if err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", err)
	}

	count, ch = 0, conn.EntryChannel()
	go doquery(int64(1000), conn)
	time.Sleep(1 * time.Second)
	numFiles := len(getbackfillFiles(backfillDir()))
	if numFiles != 1 {
		e := fmt.Errorf("expected one backfill file, but found %d files", numFiles)
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", e)
	}
	now = time.Now()
	for range ch {
		count++
	}

	log.Printf("limit=1000,chsize=10; received %v items; took %v\n",
		count, time.Since(now))

	if len(getbackfillFiles(backfillDir())) > 0 {
		e := fmt.Errorf("Expected backfill file to be deleted")
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", e)
	} else if ctxt.err != nil {
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", ctxt.err)
	}

	// ******* Case 4
	// file should be created and error out due to exceeding backfill
	// directory size limit, and file is deleted
	// cap(ch) == 256 & limit 2000 & 2 concur request, read slow,
	cleanbackfillFiles()
	n1ftyConfig, err := GetN1ftyConfig()
	if err != nil {
		t.Errorf("GetN1ftyConfig err: %v", err)
	}
	kv := make(map[string]interface{}, 1)
	kv[n1ftyTmpSpaceLimitKey] = int64(1)
	err = n1ftyConfig.SetConfig(kv)
	if err != nil {
		t.Errorf("GetN1ftyConfig err: %v", err)
	}

	concur := 30
	donech := make(chan *qcmdContext, concur)
	for i := 0; i < concur; i++ {
		go func(donech chan *qcmdContext) {
			ctxt := &qcmdContext{}
			conn, err := datastore.NewSizedIndexConnection(100, ctxt)
			if err != nil {
				t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled failed ", err)
			}

			count, ch := 0, conn.EntryChannel()
			go doquery(int64(2000), conn)
			now := time.Now()
			for range ch {
				time.Sleep(20 * time.Millisecond)
				count++
			}
			log.Printf("%d limit=1000,chsize=256; received %v items; took %v\n",
				i, count, time.Since(now))
			donech <- ctxt
		}(donech)
	}
	// wait for it to complete
	for i := 0; i < concur; i++ {
		ctxt := <-donech
		if ctxt.err == nil {
			t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled expected error", nil)
		}
	}
	//time.Sleep(1 * time.Second)
	numFiles = len(getbackfillFiles(backfillDir()))
	if numFiles > 0 {
		e := fmt.Errorf("Expected backfill file to be deleted, but found %d files", numFiles)
		t.Fatalf("%v: %v\n", "TestFTSBufferedScan_BackfillEnabled expected error", e)
	}

	kv[n1ftyTmpSpaceLimitKey] = int64(100)
	n1ftyConfig.SetConfig(kv)
}

func cleanbackfillFiles() {
	dir := backfillDir()
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), N1FTYBACKFILLPREFIX) {
			log.Printf("cleaning files %s", file.Name())
			os.Remove(dir + "/" + file.Name())
		}
	}
}

func backfillDir() string {
	file, err := ioutil.TempFile("" /*dir*/, N1FTYBACKFILLPREFIX)
	if err != nil {
		//.HandleError(err, "Error in getting backfill dir")
	}
	dir := path.Dir(file.Name())
	os.Remove(file.Name()) // remove this file.
	log.Printf("backfillDir %s", dir)
	return dir
}

func getbackfillFiles(dir string) []string {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	rv := make([]string, 0)
	for _, file := range files {
		fname := path.Join(dir, file.Name())
		if strings.Contains(fname, N1FTYBACKFILLPREFIX) {
			rv = append(rv, fname)
		}
	}
	log.Printf("backfill files found %+v", rv)
	return rv
}

type qcmdContext struct {
	err error
}

func (ctxt *qcmdContext) GetScanCap() int64 {
	return 512 // Default index scan request size
}

func (ctxt *qcmdContext) Error(err qerrors.Error) {
	ctxt.err = err
	fmt.Printf("Scan error: %v\n", err)
}

func (ctxt *qcmdContext) Warning(wrn qerrors.Error) {
	fmt.Printf("scan warning: %v\n", wrn)
}

func (ctxt *qcmdContext) Fatal(fatal qerrors.Error) {
	fmt.Printf("scan fatal: %v\n", fatal)
}

func (ctxt *qcmdContext) MaxParallelism() int {
	return 1
}
