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
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

func TestShowArrowBatchData(t *testing.T) {
	arrowFields := []arrow.Field{
		{
			Name: "show_string",
			Type: &arrow.StringType{},
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer arrowWriter.Close()

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	recordBuilder.Field(0).(*array.StringBuilder).Append("str1a\nstr1b")
	recordBuilder.Field(0).(*array.StringBuilder).Append("str2")

	record := recordBuilder.NewRecord()
	defer record.Release()

	err := arrowWriter.Write(record)
	require.Nil(t, err)

	// Convert the data
	record, err = types.ReadArrowBatchToRecord(buf.Bytes(), nil)
	require.NoError(t, err)

	table := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})
	values, err := types.ReadArrowTableToRows(table)
	require.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, []any{"str1a\nstr1b"}, values[0].Values())
	assert.Equal(t, []any{"str2"}, values[1].Values())
}

func TestReadArrowRecord(t *testing.T) {
	arrowFields := []arrow.Field{
		{
			Name: "boolean_column",
			Type: &arrow.BooleanType{},
		},
		{
			Name: "int8_column",
			Type: &arrow.Int8Type{},
		},
		{
			Name: "int16_column",
			Type: &arrow.Int16Type{},
		},
		{
			Name: "int32_column",
			Type: &arrow.Int32Type{},
		},
		{
			Name: "int64_column",
			Type: &arrow.Int64Type{},
		},
		{
			Name: "float16_column",
			Type: &arrow.Float16Type{},
		},
		{
			Name: "float32_column",
			Type: &arrow.Float32Type{},
		},
		{
			Name: "float64_column",
			Type: &arrow.Float64Type{},
		},
		{
			Name: "decimal128_column",
			Type: &arrow.Decimal128Type{},
		},
		{
			Name: "decimal256_column",
			Type: &arrow.Decimal256Type{},
		},
		{
			Name: "string_column",
			Type: &arrow.StringType{},
		},
		{
			Name: "binary_column",
			Type: &arrow.BinaryType{},
		},
		{
			Name: "timestamp_column",
			Type: &arrow.TimestampType{},
		},
		{
			Name: "date64_column",
			Type: &arrow.Date64Type{},
		},
		{
			Name: "array_int64_column",
			Type: arrow.ListOf(arrow.PrimitiveTypes.Int64),
		},
		{
			Name: "map_string_int32",
			Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer arrowWriter.Close()

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	i := 0
	recordBuilder.Field(i).(*array.BooleanBuilder).Append(false)
	recordBuilder.Field(i).(*array.BooleanBuilder).Append(true)

	i++
	recordBuilder.Field(i).(*array.Int8Builder).Append(1)
	recordBuilder.Field(i).(*array.Int8Builder).Append(2)

	i++
	recordBuilder.Field(i).(*array.Int16Builder).Append(10)
	recordBuilder.Field(i).(*array.Int16Builder).Append(20)

	i++
	recordBuilder.Field(i).(*array.Int32Builder).Append(100)
	recordBuilder.Field(i).(*array.Int32Builder).Append(200)

	i++
	recordBuilder.Field(i).(*array.Int64Builder).Append(1000)
	recordBuilder.Field(i).(*array.Int64Builder).Append(2000)

	i++
	recordBuilder.Field(i).(*array.Float16Builder).Append(float16.New(10000.1))
	recordBuilder.Field(i).(*array.Float16Builder).Append(float16.New(20000.1))

	i++
	recordBuilder.Field(i).(*array.Float32Builder).Append(100000.1)
	recordBuilder.Field(i).(*array.Float32Builder).Append(200000.1)

	i++
	recordBuilder.Field(i).(*array.Float64Builder).Append(1000000.1)
	recordBuilder.Field(i).(*array.Float64Builder).Append(2000000.1)

	i++
	recordBuilder.Field(i).(*array.Decimal128Builder).Append(decimal128.FromI64(10000000))
	recordBuilder.Field(i).(*array.Decimal128Builder).Append(decimal128.FromI64(20000000))

	i++
	recordBuilder.Field(i).(*array.Decimal256Builder).Append(decimal256.FromI64(100000000))
	recordBuilder.Field(i).(*array.Decimal256Builder).Append(decimal256.FromI64(200000000))

	i++
	recordBuilder.Field(i).(*array.StringBuilder).Append("str1")
	recordBuilder.Field(i).(*array.StringBuilder).Append("str2")

	i++
	recordBuilder.Field(i).(*array.BinaryBuilder).Append([]byte("bytes1"))
	recordBuilder.Field(i).(*array.BinaryBuilder).Append([]byte("bytes2"))

	i++
	recordBuilder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(1686981953115000))
	recordBuilder.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(1686981953116000))

	i++
	recordBuilder.Field(i).(*array.Date64Builder).Append(arrow.Date64(1686981953117000))
	recordBuilder.Field(i).(*array.Date64Builder).Append(arrow.Date64(1686981953118000))

	i++
	lb := recordBuilder.Field(i).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.Int64Builder).Append(1)
	lb.ValueBuilder().(*array.Int64Builder).Append(-999231)

	lb.Append(true)
	lb.ValueBuilder().(*array.Int64Builder).Append(1)
	lb.ValueBuilder().(*array.Int64Builder).Append(2)
	lb.ValueBuilder().(*array.Int64Builder).Append(3)

	i++
	mb := recordBuilder.Field(i).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("key1")
	mb.ItemBuilder().(*array.Int32Builder).Append(1)

	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("key2")
	mb.ItemBuilder().(*array.Int32Builder).Append(2)

	record := recordBuilder.NewRecord()
	defer record.Release()

	table := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})
	values, err := types.ReadArrowTableToRows(table)
	require.Nil(t, err)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, []any{
		false, int8(1), int16(10), int32(100), int64(1000),
		float16.New(10000.1), float32(100000.1), 1000000.1,
		decimal128.FromI64(10000000), decimal256.FromI64(100000000),
		"str1", []byte("bytes1"),
		arrow.Timestamp(1686981953115000), arrow.Date64(1686981953117000),
		[]any{int64(1), int64(-999231)},
		map[any]any{"key1": int32(1)},
	},
		values[0].Values())
	assert.Equal(t, []any{
		true, int8(2), int16(20), int32(200), int64(2000),
		float16.New(20000.1), float32(200000.1), 2000000.1,
		decimal128.FromI64(20000000), decimal256.FromI64(200000000),
		"str2", []byte("bytes2"),
		arrow.Timestamp(1686981953116000), arrow.Date64(1686981953118000),
		[]any{int64(1), int64(2), int64(3)},
		map[any]any{"key2": int32(2)},
	},
		values[1].Values())
}

func TestReadArrowRecord_UnsupportedType(t *testing.T) {
	arrowFields := []arrow.Field{
		{
			Name: "unsupported_type_column",
			Type: &arrow.MonthIntervalType{},
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer arrowWriter.Close()

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	recordBuilder.Field(0).(*array.MonthIntervalBuilder).Append(1)

	record := recordBuilder.NewRecord()
	defer record.Release()

	table := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})
	_, err := types.ReadArrowTableToRows(table)
	require.NotNil(t, err)
}

func TestConvertProtoDataTypeToDataType(t *testing.T) {
	booleanDataType := &proto.DataType{
		Kind: &proto.DataType_Boolean_{},
	}
	assert.Equal(t, "Boolean", types.ConvertProtoDataTypeToDataType(booleanDataType).TypeName())

	byteDataType := &proto.DataType{
		Kind: &proto.DataType_Byte_{},
	}
	assert.Equal(t, "Byte", types.ConvertProtoDataTypeToDataType(byteDataType).TypeName())

	shortDataType := &proto.DataType{
		Kind: &proto.DataType_Short_{},
	}
	assert.Equal(t, "Short", types.ConvertProtoDataTypeToDataType(shortDataType).TypeName())

	integerDataType := &proto.DataType{
		Kind: &proto.DataType_Integer_{},
	}
	assert.Equal(t, "Integer", types.ConvertProtoDataTypeToDataType(integerDataType).TypeName())

	longDataType := &proto.DataType{
		Kind: &proto.DataType_Long_{},
	}
	assert.Equal(t, "Long", types.ConvertProtoDataTypeToDataType(longDataType).TypeName())

	floatDataType := &proto.DataType{
		Kind: &proto.DataType_Float_{},
	}
	assert.Equal(t, "Float", types.ConvertProtoDataTypeToDataType(floatDataType).TypeName())

	doubleDataType := &proto.DataType{
		Kind: &proto.DataType_Double_{},
	}
	assert.Equal(t, "Double", types.ConvertProtoDataTypeToDataType(doubleDataType).TypeName())

	decimalDataType := &proto.DataType{
		Kind: &proto.DataType_Decimal_{},
	}
	assert.Equal(t, "Decimal", types.ConvertProtoDataTypeToDataType(decimalDataType).TypeName())

	stringDataType := &proto.DataType{
		Kind: &proto.DataType_String_{},
	}
	assert.Equal(t, "String", types.ConvertProtoDataTypeToDataType(stringDataType).TypeName())

	binaryDataType := &proto.DataType{
		Kind: &proto.DataType_Binary_{},
	}
	assert.Equal(t, "Binary", types.ConvertProtoDataTypeToDataType(binaryDataType).TypeName())

	timestampDataType := &proto.DataType{
		Kind: &proto.DataType_Timestamp_{},
	}
	assert.Equal(t, "Timestamp", types.ConvertProtoDataTypeToDataType(timestampDataType).TypeName())

	timestampNtzDataType := &proto.DataType{
		Kind: &proto.DataType_TimestampNtz{},
	}
	assert.Equal(t, "TimestampNtz", types.ConvertProtoDataTypeToDataType(timestampNtzDataType).TypeName())

	dateDataType := &proto.DataType{
		Kind: &proto.DataType_Date_{},
	}
	assert.Equal(t, "Date", types.ConvertProtoDataTypeToDataType(dateDataType).TypeName())
}

func TestConvertProtoDataTypeToDataType_UnsupportedType(t *testing.T) {
	unsupportedDataType := &proto.DataType{
		Kind: &proto.DataType_YearMonthInterval_{},
	}
	assert.Equal(t, "Unsupported", types.ConvertProtoDataTypeToDataType(unsupportedDataType).TypeName())
}
