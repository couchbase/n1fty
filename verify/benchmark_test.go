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

package verify

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/value"

	mo "github.com/couchbase/moss"
)

func initIndexAndDocs(index string, kvConfig map[string]interface{},
	b *testing.B) (bleve.Index, mapping.IndexMapping, []value.Value) {
	idxMapping := bleve.NewIndexMapping()

	idx, err := bleve.NewUsing("", idxMapping, upsidedown.Name, index, kvConfig)
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
	benchmarkUpdates(gtreap.Name, b)
}

func BenchmarkInMemGtreapUpdateAndSearch(b *testing.B) {
	benchmarkUpdateAndSearch(gtreap.Name, b)
}

func BenchmarkInMemGtreapUpdateSearchAndDelete(b *testing.B) {
	benchmarkUpdateSearchAndDelete(gtreap.Name, b)
}

func BenchmarkInMemMossIndexUpdates(b *testing.B) {
	benchmarkUpdates(moss.Name, b)
}

func BenchmarkInMemMossIndexUpdateAndSearch(b *testing.B) {
	benchmarkUpdateAndSearch(moss.Name, b)
}

func BenchmarkInMemMossIndexUpdateSearchAndDelete(b *testing.B) {
	benchmarkUpdateSearchAndDelete(moss.Name, b)
}

func benchmarkUpdates(index string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(index, nil, b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("k", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUpdateAndSearch(index string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(index, nil, b)
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

func benchmarkUpdateSearchAndDelete(index string, b *testing.B) {
	idx, _, docs := initIndexAndDocs(index, nil, b)
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
	benchmarkMossOptimizable(b, KVConfigForMoss(), true, false)
}

func BenchmarkMossWithOptimizeResetAndUpdate(b *testing.B) {
	benchmarkMossOptimizable(b, KVConfigForMoss(), true, true)
}

func benchmarkMossOptimizable(b *testing.B,
	kvConfig map[string]interface{}, optimizeReset, optimizeUpdate bool) {
	oldSkipStats := mo.SkipStats
	mo.SkipStats = true
	defer func() {
		mo.SkipStats = oldSkipStats
	}()

	idx, m, docs := initIndexAndDocs(moss.Name, kvConfig, b)
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
