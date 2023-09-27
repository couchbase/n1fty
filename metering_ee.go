//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package n1fty

import (
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/config"
	"github.com/couchbase/regulator/metering"
	"github.com/couchbase/regulator/utils"
)

func getThrottleLimit(ctx regulator.Ctx) utils.Limit {
	bCtx, _ := ctx.(regulator.BucketCtx)
	rCfg := config.GetConfig()
	searchHandle := config.ResolveSettingsHandle(regulator.Search, ctx)
	return rCfg.GetConfiguredLimitForBucket(bCtx.Bucket(), searchHandle)
}

func getReadUnits(bucket string, bytes uint64) (uint64, error) {
	rus, err := metering.SearchReadToRU(bytes)
	if err != nil {
		return 0, err
	}
	context := regulator.NewBucketCtx(bucket)

	// Note that this capping of read units is still under inspection
	// so its still a WIP. Also, currently keeping the max indexes
	// per bucket to the default value as of now.
	// This capping logic is essentially going to be resolved when
	// we bring done the huge deficit issue that's highlighted (MB-54505)
	throttleLimit := uint64(getThrottleLimit(context))
	if rus.Whole() > throttleLimit {
		maxIndexCountPerSource := 20
		rus, err = regulator.NewUnits(regulator.Search, 0,
			throttleLimit/uint64(maxIndexCountPerSource*10))
		if err != nil {
			return 0, err
		}
	}

	return rus.Whole(), err
}
