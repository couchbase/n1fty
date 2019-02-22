package n1fty

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
)

func setupSampleIndex(idef []byte) (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(idef, &indexDef)
	if err != nil {
		return nil, err
	}

	searchFieldsMap, dynamicMapping, defaultAnalyzer :=
		util.SearchableFieldsForIndexDef(indexDef)
	if searchFieldsMap != nil || dynamicMapping {
		return newFTSIndex(searchFieldsMap, dynamicMapping,
			defaultAnalyzer, indexDef, nil)
	}

	return nil, fmt.Errorf("failed to setup index")
}

func TestIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleCustomIndexDef)
	if err != nil {
		t.Fatal(err)
	}

	field := "`reviews.review.content`"
	query := expression.NewConstant(`countryX:"United" america reviews.id:"10"`)

	count, indexedCount, exact, _, n1qlErr := index.Sargable(field, query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	// The index definition carries:
	// - country
	// - countryX
	// - reviews.review (dynamic)
	// - reviews.id
	// Although the index definition doesn't carry the exact
	// reviews.review.content which is what we are searching for,
	// this query will be sargable as reviews.review is a dynamic mapping.
	if !exact {
		t.Fatalf("Expected the query to be sargable")
	}

	if count != 3 {
		t.Fatalf("Expected sargable count of 3, but got: %v", count)
	}

	if indexedCount != 4 {
		t.Fatalf("Expected indexed count of 4, but got: %v", indexedCount)
	}
}

func TestDynamicIndexSargability(t *testing.T) {
	index, err := setupSampleIndex(util.SampleIndexDefWithAnalyzerEN)
	if err != nil {
		t.Fatal(err)
	}

	query := expression.NewConstant(map[string]interface{}{
		"match":    "United States",
		"field":    "country",
		"analyzer": "default",
	})

	count, _, _, _, n1qlErr := index.Sargable("", query,
		expression.NewConstant(``), nil)
	if n1qlErr != nil {
		t.Fatal(n1qlErr)
	}

	if count != 0 {
		t.Fatalf("Expected count of 0, as query is not sargable for index,"+
			" but got: %v", count)
	}
}
