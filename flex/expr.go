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
	"fmt"

	"github.com/couchbase/query/expression"
)

// Allows apps to declare supported expressions to FlexSargable().
type SupportedExpr interface {
	// Checks whether the SupportedExpr can handle an expr.
	Supports(fi *FlexIndex, identifiers Identifiers,
		expr expression.Expression, exprFieldTypes FieldTypes) (
		matches bool, fieldTracks FieldTracks, needsFiltering bool,
		flexBuild *FlexBuild, err error)
}

// ----------------------------------------------------------------------

type SupportedExprNoop struct{} // A supported expression that never matches.

func (s *SupportedExprNoop) Supports(fi *FlexIndex, identifiers Identifiers,
	expr expression.Expression, exprFieldTypes FieldTypes) (
	bool, FieldTracks, bool, *FlexBuild, error) {
	fmt.Printf("SupportedExprNoop, expr: %+v, %#v\n", expr, expr)
	return false, nil, false, nil, nil
}
