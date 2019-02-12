//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package flex

import (
	"github.com/couchbase/query/algebra"
)

// Recursively pushes the bindings from UNNEST clauses onto the identifiers.
func PushUnnests(ids Identifiers, f algebra.FromTerm) (Identifiers, error) {
	if f == nil {
		return ids, nil
	}

	if j, ok := f.(algebra.JoinTerm); ok { // Left-most UNNEST pushed first.
		var err error
		ids, err = PushUnnests(ids, j.Left())
		if err != nil {
			return nil, err
		}
	}

	u, ok := f.(*algebra.Unnest)
	if ok && !u.Outer() {
		suffix, ok := ExpressionFieldPathSuffix(ids, u.Expression(), nil, nil)
		if ok {
			rootIdentifier := ids[len(ids)-1].Name

			ids = append(Identifiers{Identifier{
				Name:      u.Alias(),
				Expansion: append([]string{rootIdentifier}, suffix...),
			}}, ids...)
		}
	}

	return ids, nil
}
