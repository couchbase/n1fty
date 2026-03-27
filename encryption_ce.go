//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !enterprise
// +build !enterprise

package n1fty

// make a writer callback for the given cipher, key and context
func MakeWriterCallback(cipher string, key []byte, context string,
) (func(data []byte) []byte, error) {
	return func(data []byte) []byte {
		return data
	}, nil
}

// make a reader callback for the given cipher, key and context
func MakeReaderCallback(cipher string, key []byte, context string,
) (func(data []byte) ([]byte, error), error) {
	return func(data []byte) ([]byte, error) {
		return data, nil
	}, nil
}
