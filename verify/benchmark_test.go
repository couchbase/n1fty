// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package verify

import (
	"math"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/sear"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/value"

	mo "github.com/couchbase/moss"
)

func kvConfigForMoss() map[string]interface{} {
	return map[string]interface{}{
		"mossCollectionOptions": map[string]interface{}{
			"MaxPreMergerBatches": math.MaxInt32,
		},
	}
}

type CollectionHolder interface {
	Collection() mo.Collection
}

type ResetStackDirtyToper interface {
	ResetStackDirtyTop() error
}

func initIndexAndDocs(indexType, kvstore string, kvConfig map[string]interface{},
	b *testing.B) (bleve.Index, mapping.IndexMapping, []value.Value) {
	idxMapping := bleve.NewIndexMapping()

	idx, err := bleve.NewUsing("", idxMapping, indexType, kvstore, kvConfig)
	if err != nil {
		b.Fatal(err)
	}

	docs := []value.Value{
		value.NewValue(map[string]interface{}{
			"dept":      "ABCDE",
			"expertise": "FGHIJ",
			"id":        "123",
		}),
		value.NewValue(map[string]interface{}{
			"dept":      "KLMNO",
			"expertise": "PQRST",
			"id":        "456",
		}),
		value.NewValue(map[string]interface{}{
			"dept":      "UVWXY",
			"expertise": "ZABCD",
			"id":        "789",
		}),
		value.NewValue(map[string]interface{}{
			"dept":      "EFGHI",
			"expertise": "JKLMN",
			"id":        "123",
		}),
	}

	return idx, idxMapping, docs
}

func fetchSearchRequest(b *testing.B) *bleve.SearchRequest {
	q := value.NewValue(`id:"123"`)
	qq, err := util.BuildQuery("", q)
	if err != nil {
		b.Fatal(err)
	}

	return bleve.NewSearchRequest(qq)
}

func BenchmarkInMemGtreapUpdates(b *testing.B) {
	benchmarkUpdates(upsidedown.Name, gtreap.Name, b)
}

func BenchmarkInMemGtreapUpdateAndSearch(b *testing.B) {
	benchmarkUpdateAndSearch(upsidedown.Name, gtreap.Name, b)
}

func BenchmarkInMemGtreapUpdateSearchAndDelete(b *testing.B) {
	benchmarkUpdateSearchAndDelete(upsidedown.Name, gtreap.Name, b)
}

func BenchmarkInMemMossIndexUpdates(b *testing.B) {
	benchmarkUpdates(upsidedown.Name, moss.Name, b)
}

func BenchmarkInMemMossIndexUpdateAndSearch(b *testing.B) {
	benchmarkUpdateAndSearch(upsidedown.Name, moss.Name, b)
}

func BenchmarkInMemMossIndexUpdateSearchAndDelete(b *testing.B) {
	benchmarkUpdateSearchAndDelete(upsidedown.Name, moss.Name, b)
}

func BenchmarkSearIndexUpdates(b *testing.B) {
	benchmarkUpdates(sear.Name, sear.Name, b)
}

func BenchmarkSearIndexUpdateAndSearch(b *testing.B) {
	benchmarkUpdateAndSearch(sear.Name, sear.Name, b)
}

func BenchmarkSearIndexUpdateSearchAndDelete(b *testing.B) {
	benchmarkUpdateSearchAndDelete(sear.Name, sear.Name, b)
}

func benchmarkUpdates(indexType, kvstore string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(indexType, kvstore, nil, b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("k", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUpdateAndSearch(indexType, kvstore string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(indexType, kvstore, nil, b)
	sr := fetchSearchRequest(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("k", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}

		_, err = idx.Search(sr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUpdateSearchAndDelete(indexType, kvstore string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(indexType, kvstore, nil, b)
	sr := fetchSearchRequest(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("k", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}

		_, err = idx.Search(sr)
		if err != nil {
			b.Fatal(err)
		}

		idx.Delete("k")
	}
}

func BenchmarkMossWithoutOptimizations(b *testing.B) {
	benchmarkMossOptimizable(b, nil, false, false)
}

func BenchmarkMossWithOptimizeReset(b *testing.B) {
	benchmarkMossOptimizable(b, kvConfigForMoss(), true, false)
}

func BenchmarkMossWithOptimizeResetAndUpdate(b *testing.B) {
	benchmarkMossOptimizable(b, kvConfigForMoss(), true, true)
}

func benchmarkMossOptimizable(b *testing.B,
	kvConfig map[string]interface{}, optimizeReset, optimizeUpdate bool) {
	oldSkipStats := mo.SkipStats
	mo.SkipStats = true
	defer func() {
		mo.SkipStats = oldSkipStats
	}()

	idx, m, docs := initIndexAndDocs(upsidedown.Name, moss.Name, kvConfig, b)
	sr := fetchSearchRequest(b)

	bleveIndex, _ := idx.Advanced()
	udc := bleveIndex.(*upsidedown.UpsideDownCouch)
	kvstore, _ := udc.Advanced()
	collh := kvstore.(CollectionHolder)
	coll := collh.Collection()

	rsdt := coll.(ResetStackDirtyToper)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if optimizeUpdate {
			doc := document.NewDocument("k")

			err := m.MapDocument(doc, docs[i%len(docs)].Actual())
			if err != nil {
				b.Fatal(err)
			}

			err = udc.UpdateWithAnalysis(doc, udc.Analyze(doc), nil)
			if err != nil {
				b.Fatal(err)
			}
		} else {
			err := idx.Index("k", docs[i%len(docs)].Actual())
			if err != nil {
				b.Fatal(err)
			}
		}

		_, err := idx.Search(sr)
		if err != nil {
			b.Fatal(err)
		}

		if optimizeReset {
			_ = rsdt.ResetStackDirtyTop()
		}
	}
}

func BenchmarkVerifyEvalWithParallelism(b *testing.B) {
	parallelism := uint32(4)
	idxs := initIdxsQueue(parallelism)
	for p := uint32(0); p < parallelism; p++ {
		idx, err := bleve.NewUsing("", bleve.NewIndexMapping(), sear.Name, sear.Name, nil)
		if err != nil {
			b.Fatal(err)
		}

		idxs.enqueue(idx)
	}

	var wg sync.WaitGroup

	b.ResetTimer()

	for p := uint32(0); p < parallelism; p++ {
		wg.Add(1)
		go func(w *sync.WaitGroup) {
			for i := 0; i < b.N; i++ {
				idx := idxs.dequeue()
				idxs.enqueue(idx)
			}
			w.Done()
		}(&wg)
	}

	wg.Wait()
}
