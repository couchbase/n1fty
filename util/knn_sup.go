// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

//go:build vectors
// +build vectors

package util

import (
	"encoding/json"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/couchbase/cbft"
)

func ExtractKNNQueryFields(sr *cbft.SearchRequest,
	queryFields map[SearchField]struct{}) (map[SearchField]struct{}, error) {
	if sr != nil && sr.KNN != nil {
		var knn []*bleve.KNNRequest
		var err error
		if err = json.Unmarshal(sr.KNN, &knn); err != nil {
			return nil, err
		}

		for _, entry := range knn {
			if entry != nil {
				if entry.Vector == nil && entry.VectorBase64 != "" {
					entry.Vector, err = document.DecodeVector(entry.VectorBase64)
					if err != nil {
						return nil, err
					}
				}
				queryFields[SearchField{
					Name: entry.Field,
					Type: "vector",
					Dims: len(entry.Vector),
				}] = struct{}{}
			}
		}
	}

	return queryFields, nil
}
