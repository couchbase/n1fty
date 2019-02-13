package n1fty

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
)

func setupSampleIndex() (*FTSIndex, error) {
	var indexDef *cbgt.IndexDef
	err := json.Unmarshal(util.SampleIndexDef, &indexDef)
	if err != nil {
		return nil, err
	}

	searchableFieldsTypeMap, dynamicMapping := util.SearchableFieldsForIndexDef(indexDef)
	if searchableFieldsTypeMap != nil || dynamicMapping {
		return newFTSIndex(searchableFieldsTypeMap, dynamicMapping, indexDef, nil)
	}

	return nil, fmt.Errorf("failed to setup index")
}

func TestIndexSargability(t *testing.T) {
	index, err := setupSampleIndex()
	if err != nil {
		t.Fatal(err)
	}

	field := "`reviews.review.content`"
	query := expression.NewConstant(`countryX:"United" america reviews.id:"10"`)

	count, sargable, n1qlErr := index.Sargable(field, query, nil)
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
	if !sargable {
		t.Fatalf("Expected the query to be sargable")
	}

	// Expects sargable count of only 1, because:
	// - reviews.review .. de analyzer in definition, but standard in query
	// - countryX .. fa analyzer in definition, but standard in query
	// - reviews.id .. standard analyzer in definition and query
	if count != 1 {
		t.Fatalf("Expected to get a count of 1, but got: %v", count)
	}
}
