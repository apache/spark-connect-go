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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
)

func TestStructOf(t *testing.T) {
	s := StructOf(NewStructField("col1", BYTE))
	assert.Len(t, s.Fields, 1)
}

func TestStructType_TypeName(t *testing.T) {
	structType := StructType{
		Fields: []StructField{
			{Name: "field1", DataType: INTEGER},
			{Name: "field2", DataType: STRING},
		},
	}
	assert.Equal(t, "structtype", structType.TypeName())
}

func TestStructType_IsNumeric(t *testing.T) {
	structType := StructType{
		Fields: []StructField{
			{Name: "field1", DataType: INTEGER},
		},
	}
	assert.False(t, structType.IsNumeric())
}

func TestStructType_ToArrowType(t *testing.T) {
	tests := []struct {
		name       string
		structType StructType
		validate   func(t *testing.T, arrowType arrow.DataType)
	}{
		{
			name: "Simple struct with integer and string fields",
			structType: StructType{
				Fields: []StructField{
					{Name: "id", DataType: INTEGER, Nullable: false},
					{Name: "name", DataType: STRING, Nullable: true},
				},
			},
			validate: func(t *testing.T, arrowType arrow.DataType) {
				structType, ok := arrowType.(*arrow.StructType)
				assert.True(t, ok)
				assert.Equal(t, 2, structType.NumFields())

				field1 := structType.Field(0)
				assert.Equal(t, "id", field1.Name)
				assert.Equal(t, arrow.PrimitiveTypes.Int32, field1.Type)
				assert.False(t, field1.Nullable)

				field2 := structType.Field(1)
				assert.Equal(t, "name", field2.Name)
				assert.Equal(t, arrow.BinaryTypes.String, field2.Type)
				assert.True(t, field2.Nullable)
			},
		},
		{
			name: "Struct with array field",
			structType: StructType{
				Fields: []StructField{
					{Name: "items", DataType: ArrayType{ElementType: STRING, ContainsNull: true}, Nullable: false},
				},
			},
			validate: func(t *testing.T, arrowType arrow.DataType) {
				structType, ok := arrowType.(*arrow.StructType)
				assert.True(t, ok)
				assert.Equal(t, 1, structType.NumFields())

				field := structType.Field(0)
				assert.Equal(t, "items", field.Name)
				listType, ok := field.Type.(*arrow.ListType)
				assert.True(t, ok)
				assert.Equal(t, arrow.BinaryTypes.String, listType.Elem())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrowType := tt.structType.ToArrowType()
			tt.validate(t, arrowType)
		})
	}
}

func TestStructType_ToArrowType_ReturnType(t *testing.T) {
	// Test that ToArrowType returns arrow.DataType interface, not *arrow.StructType
	structType := StructType{
		Fields: []StructField{
			{Name: "field1", DataType: INTEGER},
		},
	}

	arrowType := structType.ToArrowType()

	// This should compile and work correctly with the interface
	dataType := arrowType
	assert.NotNil(t, dataType)

	// But we should still be able to cast it to the concrete type
	concreteType, ok := arrowType.(*arrow.StructType)
	assert.True(t, ok)
	assert.Equal(t, 1, concreteType.NumFields())
}

func TestTreeString(t *testing.T) {
	c := NewStructField("col1", STRING)
	c.Nullable = false
	s := StructOf(
		c,
		NewStructField("col2", INTEGER),
		NewStructField("col3", DATE),
	)
	assert.Len(t, s.Fields, 3)
	ts := s.TreeString()
	assert.Contains(t, ts, "|-- col1: string (nullable = false")
	assert.Contains(t, ts, "|-- col2: integer (nullable = true)")
	assert.Contains(t, ts, "|-- col3: date (nullable = true)")
}

func TestTreeString_ComplexNestedTypes(t *testing.T) {
	// Create a complex nested structure with maps, arrays, and nested structs
	nestedStruct := StructOf(
		NewStructField("nested_id", INTEGER),
		NewStructField("nested_name", STRING),
	)

	arrayOfStrings := ArrayType{
		ElementType:  STRING,
		ContainsNull: true,
	}

	mapOfIntToString := MapType{
		KeyType:           INTEGER,
		ValueType:         STRING,
		ValueContainsNull: true,
	}

	arrayOfMaps := ArrayType{
		ElementType: MapType{
			KeyType:           STRING,
			ValueType:         DOUBLE,
			ValueContainsNull: false,
		},
		ContainsNull: true,
	}

	complexStruct := StructOf(
		NewStructField("id", INTEGER),
		NewStructField("name", STRING),
		NewStructField("tags", arrayOfStrings),
		NewStructField("metadata", mapOfIntToString),
		NewStructField("scores", arrayOfMaps),
		NewStructField("profile", *nestedStruct),
		NewStructField("active", BOOLEAN),
	)

	ts := complexStruct.TreeString()

	// Verify the tree string contains all expected elements
	assert.Contains(t, ts, "root")
	assert.Contains(t, ts, "|-- id: integer (nullable = true)")
	assert.Contains(t, ts, "|-- name: string (nullable = true)")
	assert.Contains(t, ts, "|-- tags: array (nullable = true)")
	assert.Contains(t, ts, "|-- metadata: map (nullable = true)")
	assert.Contains(t, ts, "|-- scores: array (nullable = true)")
	assert.Contains(t, ts, "|-- profile: structtype (nullable = true)")
	assert.Contains(t, ts, "|-- active: boolean (nullable = true)")

	// Verify the structure starts with "root" and ends with newlines
	assert.True(t, strings.HasPrefix(ts, "root\n"))
	assert.True(t, strings.HasSuffix(ts, "\n"))

	// Verify the correct number of field lines (excluding root and trailing newline)
	lines := strings.Split(strings.TrimSpace(ts), "\n")
	assert.Equal(t, len(complexStruct.Fields)+7, len(lines)) // root + number of fields

	// Verify the prefix format for all field lines
	for i := 1; i < len(lines); i++ {
		assert.True(t, strings.HasPrefix(lines[i], " |"))
	}
}
