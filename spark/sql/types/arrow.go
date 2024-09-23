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
	"bytes"
	"fmt"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"

	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

func ReadArrowTable(table arrow.Table) ([][]any, error) {
	numRows := table.NumRows()
	numColumns := int(table.NumCols())

	values := make([][]any, numRows)
	for i := range values {
		values[i] = make([]any, numColumns)
	}

	for columnIndex := 0; columnIndex < numColumns; columnIndex++ {
		err := ReadArrowRecordColumn(table, columnIndex, values)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// readArrowRecordColumn reads all values in a column and stores them in values
func ReadArrowRecordColumn(record arrow.Table, columnIndex int, values [][]any) error {
	chunkedColumn := record.Column(columnIndex).Data()
	dataTypeId := chunkedColumn.DataType().ID()
	switch dataTypeId {
	case arrow.BOOL:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewBooleanData(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.INT8:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewInt8Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.INT16:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewInt16Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.INT32:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewInt32Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.INT64:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewInt64Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.FLOAT16:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewFloat16Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.FLOAT32:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewFloat32Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.FLOAT64:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewFloat64Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.DECIMAL | arrow.DECIMAL128:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewDecimal128Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.DECIMAL256:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewDecimal256Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.STRING:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewStringData(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.BINARY:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewBinaryData(columnData.Data())
			for i := 0; rowIndex < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.TIMESTAMP:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewTimestampData(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	case arrow.DATE64:
		rowIndex := 0
		for _, columnData := range chunkedColumn.Chunks() {
			vector := array.NewDate64Data(columnData.Data())
			for i := 0; i < columnData.Len(); i++ {
				values[rowIndex][columnIndex] = vector.Value(i)
				rowIndex += 1
			}
		}
	default:
		return fmt.Errorf("unsupported arrow data type %s in column %d", dataTypeId.String(), columnIndex)
	}
	return nil
}

func ReadArrowBatchToRecord(data []byte, schema *StructType) (arrow.Record, error) {
	reader := bytes.NewReader(data)
	arrowReader, err := ipc.NewReader(reader)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to create arrow reader: %w", err), sparkerrors.ReadError)
	}
	defer arrowReader.Release()

	record, err := arrowReader.Read()
	record.Retain()
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to read arrow record: %w", err), sparkerrors.ReadError)
	}
	return record, nil
}

func arrowStructToProtoStruct(schema *arrow.StructType) *proto.DataType_Struct_ {
	fields := make([]*proto.DataType_StructField, schema.NumFields())
	for i, field := range schema.Fields() {
		fields[i] = &proto.DataType_StructField{
			Name:     field.Name,
			DataType: ArrowTypeToProto(field.Type),
		}
	}
	return &proto.DataType_Struct_{
		Struct: &proto.DataType_Struct{
			Fields: fields,
		},
	}
}

func ArrowTypeToProto(dataType arrow.DataType) *proto.DataType {
	switch dataType.ID() {
	case arrow.BOOL:
		return &proto.DataType{Kind: &proto.DataType_Boolean_{}}
	case arrow.INT8:
		return &proto.DataType{Kind: &proto.DataType_Byte_{}}
	case arrow.INT16:
		return &proto.DataType{Kind: &proto.DataType_Short_{}}
	case arrow.INT32:
		return &proto.DataType{Kind: &proto.DataType_Integer_{}}
	case arrow.INT64:
		return &proto.DataType{Kind: &proto.DataType_Long_{}}
	case arrow.FLOAT16:
		return &proto.DataType{Kind: &proto.DataType_Float_{}}
	case arrow.FLOAT32:
		return &proto.DataType{Kind: &proto.DataType_Double_{}}
	case arrow.FLOAT64:
		return &proto.DataType{Kind: &proto.DataType_Double_{}}
	case arrow.DECIMAL | arrow.DECIMAL128:
		return &proto.DataType{Kind: &proto.DataType_Decimal_{}}
	case arrow.DECIMAL256:
		return &proto.DataType{Kind: &proto.DataType_Decimal_{}}
	case arrow.STRING:
		return &proto.DataType{Kind: &proto.DataType_String_{}}
	case arrow.BINARY:
		return &proto.DataType{Kind: &proto.DataType_Binary_{}}
	case arrow.TIMESTAMP:
		return &proto.DataType{Kind: &proto.DataType_Timestamp_{}}
	case arrow.DATE64:
		return &proto.DataType{Kind: &proto.DataType_Date_{}}
	case arrow.LIST:
		return &proto.DataType{Kind: &proto.DataType_Array_{
			Array: &proto.DataType_Array{
				ElementType: ArrowTypeToProto(dataType.(*arrow.ListType).Elem()),
			},
		}}
	case arrow.STRUCT:
		return &proto.DataType{Kind: arrowStructToProtoStruct(dataType.(*arrow.StructType))}
	default:
		return &proto.DataType{Kind: &proto.DataType_Unparsed_{}}
	}
}

func ArrowSchemaToProto(schema *arrow.Schema) proto.DataType_Struct {
	fields := make([]*proto.DataType_StructField, schema.NumFields())
	for i, field := range schema.Fields() {
		fields[i] = &proto.DataType_StructField{
			Name:     field.Name,
			DataType: ArrowTypeToProto(field.Type),
		}
	}
	return proto.DataType_Struct{
		Fields: fields,
	}
}
