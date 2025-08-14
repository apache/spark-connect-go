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

package types_test

import (
	"testing"

	proto "github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestConvertProtoStructFieldSupported(t *testing.T) {
	protoType := &proto.DataType{Kind: &proto.DataType_Integer_{}}
	structField := &proto.DataType_StructField{
		Name:     "test",
		DataType: protoType,
		Nullable: true,
	}

	dt := types.ConvertProtoStructField(structField)
	assert.Equal(t, "test", dt.Name)
	assert.IsType(t, types.IntegerType{}, dt.DataType)
}

func TestConvertProtoStructFieldUnsupported(t *testing.T) {
	protoType := &proto.DataType{Kind: &proto.DataType_CalendarInterval_{}}
	structField := &proto.DataType_StructField{
		Name:     "test",
		DataType: protoType,
		Nullable: true,
	}

	dt := types.ConvertProtoStructField(structField)
	assert.Equal(t, "test", dt.Name)
	assert.IsType(t, types.UnsupportedType{}, dt.DataType)
}

func TestConvertProtoStructToGoStruct(t *testing.T) {
	protoType := &proto.DataType{
		Kind: &proto.DataType_Struct_{
			Struct: &proto.DataType_Struct{
				Fields: []*proto.DataType_StructField{
					{
						Name:     "test",
						DataType: &proto.DataType{Kind: &proto.DataType_Integer_{}},
						Nullable: true,
					},
				},
			},
		},
	}
	structType, err := types.ConvertProtoDataTypeToStructType(protoType)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(structType.Fields))
	assert.Equal(t, "test", structType.Fields[0].Name)
	assert.IsType(t, types.IntegerType{}, structType.Fields[0].DataType)

	// Check for input type that is not a struct type and it returns an error.
	protoType = &proto.DataType{Kind: &proto.DataType_Integer_{}}
	_, err = types.ConvertProtoDataTypeToStructType(protoType)
	assert.Error(t, err)
}

func TestConvertProtoArrayType(t *testing.T) {
	tests := []struct {
		name         string
		protoType    *proto.DataType
		expectedType types.DataType
		expectedName string
	}{
		{
			name: "Array of integers",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Array_{
					Array: &proto.DataType_Array{
						ElementType:  &proto.DataType{Kind: &proto.DataType_Integer_{}},
						ContainsNull: true,
					},
				},
			},
			expectedType: types.ArrayType{
				ElementType:  types.INTEGER,
				ContainsNull: true,
			},
			expectedName: "Array<Integer>",
		},
		{
			name: "Array of strings without nulls",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Array_{
					Array: &proto.DataType_Array{
						ElementType:  &proto.DataType{Kind: &proto.DataType_String_{}},
						ContainsNull: false,
					},
				},
			},
			expectedType: types.ArrayType{
				ElementType:  types.STRING,
				ContainsNull: false,
			},
			expectedName: "Array<String>",
		},
		{
			name: "Nested array",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Array_{
					Array: &proto.DataType_Array{
						ElementType: &proto.DataType{
							Kind: &proto.DataType_Array_{
								Array: &proto.DataType_Array{
									ElementType:  &proto.DataType{Kind: &proto.DataType_Double_{}},
									ContainsNull: false,
								},
							},
						},
						ContainsNull: true,
					},
				},
			},
			expectedType: types.ArrayType{
				ElementType: types.ArrayType{
					ElementType:  types.DOUBLE,
					ContainsNull: false,
				},
				ContainsNull: true,
			},
			expectedName: "Array<Array<Double>>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			convertedType := types.ConvertProtoDataTypeToDataType(tt.protoType)
			assert.Equal(t, tt.expectedType, convertedType)
			assert.Equal(t, tt.expectedName, convertedType.TypeName())
		})
	}
}

func TestConvertProtoMapType(t *testing.T) {
	tests := []struct {
		name         string
		protoType    *proto.DataType
		expectedType types.DataType
		expectedName string
	}{
		{
			name: "Map of string to integer",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Map_{
					Map: &proto.DataType_Map{
						KeyType:           &proto.DataType{Kind: &proto.DataType_String_{}},
						ValueType:         &proto.DataType{Kind: &proto.DataType_Integer_{}},
						ValueContainsNull: true,
					},
				},
			},
			expectedType: types.MapType{
				KeyType:           types.STRING,
				ValueType:         types.INTEGER,
				ValueContainsNull: true,
			},
			expectedName: "Map<String,Integer>",
		},
		{
			name: "Map with array values",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Map_{
					Map: &proto.DataType_Map{
						KeyType: &proto.DataType{Kind: &proto.DataType_Integer_{}},
						ValueType: &proto.DataType{
							Kind: &proto.DataType_Array_{
								Array: &proto.DataType_Array{
									ElementType:  &proto.DataType{Kind: &proto.DataType_String_{}},
									ContainsNull: true,
								},
							},
						},
						ValueContainsNull: false,
					},
				},
			},
			expectedType: types.MapType{
				KeyType: types.INTEGER,
				ValueType: types.ArrayType{
					ElementType:  types.STRING,
					ContainsNull: true,
				},
				ValueContainsNull: false,
			},
			expectedName: "Map<Integer,Array<String>>",
		},
		{
			name: "Nested map",
			protoType: &proto.DataType{
				Kind: &proto.DataType_Map_{
					Map: &proto.DataType_Map{
						KeyType: &proto.DataType{Kind: &proto.DataType_String_{}},
						ValueType: &proto.DataType{
							Kind: &proto.DataType_Map_{
								Map: &proto.DataType_Map{
									KeyType:           &proto.DataType{Kind: &proto.DataType_String_{}},
									ValueType:         &proto.DataType{Kind: &proto.DataType_Double_{}},
									ValueContainsNull: false,
								},
							},
						},
						ValueContainsNull: true,
					},
				},
			},
			expectedType: types.MapType{
				KeyType: types.STRING,
				ValueType: types.MapType{
					KeyType:           types.STRING,
					ValueType:         types.DOUBLE,
					ValueContainsNull: false,
				},
				ValueContainsNull: true,
			},
			expectedName: "Map<String,Map<String,Double>>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			convertedType := types.ConvertProtoDataTypeToDataType(tt.protoType)
			assert.Equal(t, tt.expectedType, convertedType)
			assert.Equal(t, tt.expectedName, convertedType.TypeName())
		})
	}
}

func TestConvertComplexNestedTypes(t *testing.T) {
	// Test a complex nested structure: Struct containing Array<Map<String, Integer>>
	protoType := &proto.DataType{
		Kind: &proto.DataType_Struct_{
			Struct: &proto.DataType_Struct{
				Fields: []*proto.DataType_StructField{
					{
						Name: "complex_field",
						DataType: &proto.DataType{
							Kind: &proto.DataType_Array_{
								Array: &proto.DataType_Array{
									ElementType: &proto.DataType{
										Kind: &proto.DataType_Map_{
											Map: &proto.DataType_Map{
												KeyType:           &proto.DataType{Kind: &proto.DataType_String_{}},
												ValueType:         &proto.DataType{Kind: &proto.DataType_Integer_{}},
												ValueContainsNull: true,
											},
										},
									},
									ContainsNull: false,
								},
							},
						},
						Nullable: true,
					},
				},
			},
		},
	}

	convertedType := types.ConvertProtoDataTypeToDataType(protoType)
	structType, ok := convertedType.(types.StructType)
	assert.True(t, ok)
	assert.Equal(t, 1, len(structType.Fields))
	assert.Equal(t, "complex_field", structType.Fields[0].Name)

	arrayType, ok := structType.Fields[0].DataType.(types.ArrayType)
	assert.True(t, ok)
	assert.False(t, arrayType.ContainsNull)

	mapType, ok := arrayType.ElementType.(types.MapType)
	assert.True(t, ok)
	assert.Equal(t, types.STRING, mapType.KeyType)
	assert.Equal(t, types.INTEGER, mapType.ValueType)
	assert.True(t, mapType.ValueContainsNull)
}
