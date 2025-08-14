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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestArrayType_TypeName(t *testing.T) {
	tests := []struct {
		name         string
		arrayType    ArrayType
		expectedName string
	}{
		{
			name: "Array of integers",
			arrayType: ArrayType{
				ElementType:  INTEGER,
				ContainsNull: false,
			},
			expectedName: "Array<Integer>",
		},
		{
			name: "Array of strings with nulls",
			arrayType: ArrayType{
				ElementType:  STRING,
				ContainsNull: true,
			},
			expectedName: "Array<String>",
		},
		{
			name: "Nested array",
			arrayType: ArrayType{
				ElementType: ArrayType{
					ElementType:  DOUBLE,
					ContainsNull: false,
				},
				ContainsNull: false,
			},
			expectedName: "Array<Array<Double>>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedName, tt.arrayType.TypeName())
		})
	}
}

func TestArrayType_IsNumeric(t *testing.T) {
	arrayType := ArrayType{
		ElementType:  INTEGER,
		ContainsNull: false,
	}
	assert.False(t, arrayType.IsNumeric())
}

func TestArrayType_ToArrowType(t *testing.T) {
	tests := []struct {
		name      string
		arrayType ArrayType
		validate  func(t *testing.T, arrowType arrow.DataType)
	}{
		{
			name: "Array of integers",
			arrayType: ArrayType{
				ElementType:  INTEGER,
				ContainsNull: false,
			},
			validate: func(t *testing.T, arrowType arrow.DataType) {
				listType, ok := arrowType.(*arrow.ListType)
				assert.True(t, ok)
				assert.Equal(t, arrow.PrimitiveTypes.Int32, listType.Elem())
			},
		},
		{
			name: "Array of strings",
			arrayType: ArrayType{
				ElementType:  STRING,
				ContainsNull: true,
			},
			validate: func(t *testing.T, arrowType arrow.DataType) {
				listType, ok := arrowType.(*arrow.ListType)
				assert.True(t, ok)
				assert.Equal(t, arrow.BinaryTypes.String, listType.Elem())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType := tt.arrayType.ToArrowType()
			tt.validate(t, arrowType)
		})
	}
}

func TestMapType_TypeName(t *testing.T) {
	tests := []struct {
		name         string
		mapType      MapType
		expectedName string
	}{
		{
			name: "Map of string to integer",
			mapType: MapType{
				KeyType:           STRING,
				ValueType:         INTEGER,
				ValueContainsNull: false,
			},
			expectedName: "Map<String,Integer>",
		},
		{
			name: "Map of integer to array",
			mapType: MapType{
				KeyType: INTEGER,
				ValueType: ArrayType{
					ElementType:  STRING,
					ContainsNull: true,
				},
				ValueContainsNull: true,
			},
			expectedName: "Map<Integer,Array<String>>",
		},
		{
			name: "Nested map",
			mapType: MapType{
				KeyType: STRING,
				ValueType: MapType{
					KeyType:           STRING,
					ValueType:         DOUBLE,
					ValueContainsNull: false,
				},
				ValueContainsNull: false,
			},
			expectedName: "Map<String,Map<String,Double>>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectedName, tt.mapType.TypeName())
		})
	}
}

func TestMapType_IsNumeric(t *testing.T) {
	mapType := MapType{
		KeyType:           STRING,
		ValueType:         INTEGER,
		ValueContainsNull: false,
	}
	assert.False(t, mapType.IsNumeric())
}

func TestMapType_ToArrowType(t *testing.T) {
	tests := []struct {
		name     string
		mapType  MapType
		validate func(t *testing.T, arrowType arrow.DataType)
	}{
		{
			name: "Map of string to integer",
			mapType: MapType{
				KeyType:           STRING,
				ValueType:         INTEGER,
				ValueContainsNull: true,
			},
			validate: func(t *testing.T, arrowType arrow.DataType) {
				mapType, ok := arrowType.(*arrow.MapType)
				assert.True(t, ok)
				assert.Equal(t, arrow.BinaryTypes.String, mapType.KeyType())
				assert.Equal(t, arrow.PrimitiveTypes.Int32, mapType.ItemType())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType := tt.mapType.ToArrowType()
			tt.validate(t, arrowType)
		})
	}
}

func TestMakeArrayType(t *testing.T) {
	arrayType := MakeArrayType(STRING, true)

	assert.Equal(t, STRING, arrayType.ElementType)
	assert.True(t, arrayType.ContainsNull)
	assert.Equal(t, "Array<String>", arrayType.TypeName())
}

func TestComplexTypeNesting(t *testing.T) {
	// Test complex nested structure: Array<Map<String, Array<Integer>>>
	innerArray := ArrayType{
		ElementType:  INTEGER,
		ContainsNull: false,
	}

	mapType := MapType{
		KeyType:           STRING,
		ValueType:         innerArray,
		ValueContainsNull: true,
	}

	outerArray := ArrayType{
		ElementType:  mapType,
		ContainsNull: false,
	}

	expectedName := "Array<Map<String,Array<Integer>>>"
	assert.Equal(t, expectedName, outerArray.TypeName())
}
