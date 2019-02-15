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

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/store/moss"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/value"
)

func initIndexAndDocs(index string, b *testing.B) (
	bleve.Index, []value.Value) {
	idxMapping := bleve.NewIndexMapping()
	idx, err := bleve.NewUsing("", idxMapping, upsidedown.Name, index, nil)
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

	return idx, docs
}

func fetchSearchRequest(b *testing.B) *bleve.SearchRequest {
	q := value.NewValue(`id:"123"`)
	qq, err := util.BuildQuery("", q, nil)
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
	idx, docs := initIndexAndDocs(index, b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("temp_doc", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUpdateAndSearch(index string, b *testing.B) {
	idx, docs := initIndexAndDocs(index, b)
	sr := fetchSearchRequest(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("temp_doc", docs[i%len(docs)].Actual())
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
	idx, docs := initIndexAndDocs(index, b)
	sr := fetchSearchRequest(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := idx.Index("temp_doc", docs[i%len(docs)].Actual())
		if err != nil {
			b.Fatal(err)
		}

		_, err = idx.Search(sr)
		if err != nil {
			b.Fatal(err)
		}

		idx.Delete("temp_doc")
	}
}
