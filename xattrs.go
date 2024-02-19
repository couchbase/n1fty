//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1fty

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/n1fty/util"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/search"
	"github.com/couchbase/query/value"
)

func init() {

	search.RegisterParseXattrs(ParseXattrs)
}

var ParseXattrs = func(query expression.Expression) ([]string, error) {

	if query.Type() == value.STRING {
		qStr, ok := query.Value().Actual().(string)
		if !ok {
			return nil, fmt.Errorf("n1fty: failed to typecase " +
				"query of type value.STRING to string")
		}

		fields, err := parseXattrsFromString(qStr)
		if err != nil {
			return nil, err
		}

		return fields, nil
	} else if query.Type() == value.OBJECT {
		qBytes, err := query.Value().MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("n1fty: unable to marshal query")
		}

		fields, err := parseXattrsFromObject(qBytes)
		if err != nil {
			return nil, err
		}

		return fields, nil
	}

	return nil, nil
}

func fetchXattrsKeys(query query.Query) ([]string, error) {
	xattrsFields := make([]string, 0)
	queryFields, err := util.FetchFieldsToSearchFromQuery(query)
	if err != nil {
		return nil, fmt.Errorf("n1fty: err: %v", err)
	}

	for field, _ := range queryFields {
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

func parseXattrsFromObject(b []byte) ([]string, error) {

	var maybeSReq map[string]interface{}
	err := json.Unmarshal(b, &maybeSReq)
	if err != nil {
		return nil, fmt.Errorf("n1fty: unable to unmarshal query. err: %v", err)
	}

	val, ok := maybeSReq["query"]
	if !ok {
		return nil, fmt.Errorf("n1fty: query object has no query key")
	}

	var qBytes []byte
	_, ok = val.(map[string]interface{})
	if ok {
		qBytes, err = json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("n1fty: unable to marshal query value. err: %v", err)
		}
	} else {
		qBytes = b
	}

	query, err := util.BuildQueryFromBytes("", qBytes)
	if err != nil {
		return nil, err
	}

	return fetchXattrsKeys(query)
}

func parseXattrsFromString(s string) ([]string, error) {
	query, err := util.BuildQueryFromString("", s)
	if err != nil {
		return nil, fmt.Errorf("n1fty: err: %v", err)
	}

	return fetchXattrsKeys(query)
}
