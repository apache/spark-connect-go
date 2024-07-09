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

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
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
	structType, err = types.ConvertProtoDataTypeToStructType(protoType)
	assert.Error(t, err)
}
