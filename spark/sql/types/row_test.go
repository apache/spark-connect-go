//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// rowImplTest is a read-only sample [Row] to be used in all tests.
var rowImplSample rowImpl = rowImpl{
	values: []any{1, 2, 3, 4, 5},
	offsets: map[string]int{
		"one":   0,
		"two":   1,
		"three": 2,
		"four":  3,
		"five":  4,
	},
}

func TestRowImpl_At(t *testing.T) {
	testCases := []struct {
		name  string
		input int
		exp   any
	}{
		{
			name:  "index within range",
			input: 2,
			exp:   3,
		},
		{
			name:  "index out of range",
			input: 6,
			exp:   nil,
		},
		{
			name:  "negative index",
			input: -1,
			exp:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			act := rowImplSample.At(tc.input)
			require.Equal(t, tc.exp, act)
		})
	}
}

func TestRowImpl_Value(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		exp   any
	}{
		{
			name:  "valid field name",
			input: "two",
			exp:   2,
		},
		{
			name:  "invalid field name",
			input: "six",
			exp:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			act := rowImplSample.Value(tc.input)
			require.Equal(t, tc.exp, act)
		})
	}
}

func TestRowImpl_Values(t *testing.T) {
	exp := []any{1, 2, 3, 4, 5}
	act := rowImplSample.Values()
	require.Equal(t, exp, act)
}

func TestRowImpl_Len(t *testing.T) {
	exp := 5
	act := rowImplSample.Len()
	require.Equal(t, exp, act)
}

func TestRowImpl_FieldNames(t *testing.T) {
	exp := []string{"one", "two", "three", "four", "five"}
	act := rowImplSample.FieldNames()
	require.ElementsMatch(t, exp, act)
}
