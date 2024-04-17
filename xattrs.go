//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
	"strings"

	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/search"
)

func init() {
	search.RegisterParseXattrs(ParseXattrs)
}

var ParseXattrs = func(query expression.Expression) ([]string, error) {
	fields, _, _, _, err := util.ParseQueryToSearchRequest("", query.Value())
	if err != nil {
		return nil, util.N1QLError(err, "unable to interpret query")
	}

	return obtainXattrsFields(fields)
}

func obtainXattrsFields(fields map[util.SearchField]struct{}) ([]string, error) {
	xattrsFields := make([]string, 0)
	for field, _ := range fields {
		if strings.HasPrefix(field.Name, "_$xattrs.") {
			key := parseFirstKey(field.Name)
			if key != "" {
				xattrsFields = append(xattrsFields, key)
			}
		}
	}

	return xattrsFields, nil
}

func parseFirstKey(path string) string {
	keys := strings.Split(path, ".")
	if len(keys) >= 2 {
		return keys[1]
	} else {
		return ""
	}
}
