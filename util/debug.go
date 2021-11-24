// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package util

import (
	"os"
	"strconv"
)

var Debug = 0

func init() {
	v := os.Getenv("CB_N1FTY_DEBUG")
	if v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			Debug = i
		}
	}
}
