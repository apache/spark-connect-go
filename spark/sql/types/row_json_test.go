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
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowToJsonString(t *testing.T) {
	tests := []struct {
		name     string
		row      Row
		expected string
		hasError bool
	}{
		{
			name: "basic types",
			row: &rowImpl{
				values: []any{
					"hello",
					int32(42),
					int64(123),
					float64(3.14),
					true,
					nil,
				},
				offsets: map[string]int{
					"str_col":    0,
					"int_col":    1,
					"long_col":   2,
					"double_col": 3,
					"bool_col":   4,
					"null_col":   5,
				},
			},
			expected: `{"bool_col":true,"double_col":3.14,"int_col":42,"long_col":123,"null_col":null,"str_col":"hello"}`,
			hasError: false,
		},
		{
			name: "binary data",
			row: &rowImpl{
				values: []any{
					[]byte("hello world"),
				},
				offsets: map[string]int{
					"binary_col": 0,
				},
			},
			expected: `{"binary_col":"aGVsbG8gd29ybGQ="}`,
			hasError: false,
		},
		{
			name: "decimal types",
			row: &rowImpl{
				values: []any{
					decimal128.FromI64(123456),
					decimal256.FromI64(789012),
				},
				offsets: map[string]int{
					"decimal128_col": 0,
					"decimal256_col": 1,
				},
			},
			expected: `{"decimal128_col":"123456","decimal256_col":"789012"}`,
			hasError: false,
		},
		{
			name: "timestamp and date",
			row: &rowImpl{
				values: []any{
					arrow.Timestamp(1686981953115000), // microseconds
					arrow.Date32(19521),               // days since epoch (2023-06-13)
					arrow.Date64(1686981953115),       // milliseconds
				},
				offsets: map[string]int{
					"timestamp_col": 0,
					"date32_col":    1,
					"date64_col":    2,
				},
			},
			expected: `{"date32_col":"2023-06-13","date64_col":"2023-06-17","timestamp_col":"2023-06-17T06:05:53Z"}`,
			hasError: false,
		},
		{
			name: "arrays",
			row: &rowImpl{
				values: []any{
					[]any{1, 2, 3},
					[]any{"a", "b", "c"},
				},
				offsets: map[string]int{
					"int_array": 0,
					"str_array": 1,
				},
			},
			expected: `{"int_array":[1,2,3],"str_array":["a","b","c"]}`,
			hasError: false,
		},
		{
			name: "valid string map",
			row: &rowImpl{
				values: []any{
					map[string]any{
						"key1": "value1",
						"key2": 42,
					},
				},
				offsets: map[string]int{
					"map_col": 0,
				},
			},
			expected: `{"map_col":{"key1":"value1","key2":42}}`,
			hasError: false,
		},
		{
			name: "invalid map with non-string keys",
			row: &rowImpl{
				values: []any{
					map[any]any{
						42:     "value1",
						"key2": "value2",
					},
				},
				offsets: map[string]int{
					"map_col": 0,
				},
			},
			expected: "",
			hasError: true,
		},
		{
			name: "nested structures",
			row: &rowImpl{
				values: []any{
					[]any{
						map[string]any{
							"nested_key": "nested_value",
							"nested_num": 123,
						},
					},
				},
				offsets: map[string]int{
					"nested_col": 0,
				},
			},
			expected: `{"nested_col":[{"nested_key":"nested_value","nested_num":123}]}`,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.row.ToJsonString()

			if tt.hasError {
				assert.Error(t, err)
				assert.Empty(t, result)
			} else {
				require.NoError(t, err)

				// Verify the result is valid JSON
				var parsed map[string]any
				err = json.Unmarshal([]byte(result), &parsed)
				require.NoError(t, err)

				// Verify the expected content (comparing parsed JSON to avoid key ordering issues)
				var expected map[string]any
				err = json.Unmarshal([]byte(tt.expected), &expected)
				require.NoError(t, err)

				assert.Equal(t, expected, parsed)
			}
		})
	}
}

func TestConvertToJsonValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
		hasError bool
	}{
		{
			name:     "nil value",
			input:    nil,
			expected: nil,
			hasError: false,
		},
		{
			name:     "time.Time",
			input:    time.Date(2023, 6, 17, 10, 5, 53, 0, time.UTC),
			expected: "2023-06-17T10:05:53Z",
			hasError: false,
		},
		{
			name:     "zero time.Time",
			input:    time.Time{},
			expected: nil,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToJsonValue(tt.input)

			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
