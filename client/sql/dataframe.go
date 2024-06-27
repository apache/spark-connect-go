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

package sql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/spark-connect-go/v1/client/sparkerrors"
	proto "github.com/apache/spark-connect-go/v1/internal/generated"
)

// ResultCollector receives a stream of result rows
type ResultCollector interface {
	// WriteRow receives a single row from the data frame
	WriteRow(values []any)
}

// DataFrame is a wrapper for data frame, representing a distributed collection of data row.
type DataFrame interface {
	// WriteResult streams the data frames to a result collector
	WriteResult(ctx context.Context, collector ResultCollector, numRows int, truncate bool) error
	// Show uses WriteResult to write the data frames to the console output.
	Show(ctx context.Context, numRows int, truncate bool) error
	// Schema returns the schema for the current data frame.
	Schema(ctx context.Context) (*StructType, error)
	// Collect returns the data rows of the current data frame.
	Collect(ctx context.Context) ([]Row, error)
	// Writer returns a data frame writer, which could be used to save data frame to supported storage.
	Writer() DataFrameWriter
	// Write is an alias for Writer
	// Deprecated: Use Writer
	Write() DataFrameWriter
	// CreateTempView creates or replaces a temporary view.
	CreateTempView(ctx context.Context, viewName string, replace bool, global bool) error
	// Repartition re-partitions a data frame.
	Repartition(numPartitions int, columns []string) (DataFrame, error)
	// RepartitionByRange re-partitions a data frame by range partition.
	RepartitionByRange(numPartitions int, columns []RangePartitionColumn) (DataFrame, error)
}

type RangePartitionColumn struct {
	Name       string
	Descending bool
}

// dataFrameImpl is an implementation of DataFrame interface.
type dataFrameImpl struct {
	sparkSession sparkExecutor
	relation     *proto.Relation // TODO change to proto.Plan?
}

type consoleCollector struct {
}

func (c consoleCollector) WriteRow(values []any) {
	fmt.Println(values...)
}

func (df *dataFrameImpl) Show(ctx context.Context, numRows int, truncate bool) error {
	return df.WriteResult(ctx, &consoleCollector{}, numRows, truncate)
}

func (df *dataFrameImpl) WriteResult(ctx context.Context, collector ResultCollector, numRows int, truncate bool) error {
	truncateValue := 0
	if truncate {
		truncateValue = 20
	}
	vertical := false

	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: &proto.Relation_ShowString{
					ShowString: &proto.ShowString{
						Input:    df.relation,
						NumRows:  int32(numRows),
						Truncate: int32(truncateValue),
						Vertical: vertical,
					},
				},
			},
		},
	}

	responseClient, err := df.sparkSession.executePlan(ctx, plan)
	if err != nil {
		return sparkerrors.WithType(fmt.Errorf("failed to show dataframe: %w", err), sparkerrors.ExecutionError)
	}

	for {
		response, err := responseClient.Recv()
		if err != nil {
			return sparkerrors.WithType(fmt.Errorf("failed to receive show response: %w", err), sparkerrors.ReadError)
		}
		arrowBatch := response.GetArrowBatch()
		if arrowBatch == nil {
			continue
		}
		err = showArrowBatch(arrowBatch, collector)
		if err != nil {
			return err
		}
		return nil
	}
}

func (df *dataFrameImpl) Schema(ctx context.Context) (*StructType, error) {
	response, err := df.sparkSession.analyzePlan(ctx, df.createPlan())
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to analyze plan: %w", err), sparkerrors.ExecutionError)
	}

	responseSchema := response.GetSchema().Schema
	return convertProtoDataTypeToStructType(responseSchema)
}

func (df *dataFrameImpl) Collect(ctx context.Context) ([]Row, error) {
	responseClient, err := df.sparkSession.executePlan(ctx, df.createPlan())
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to execute plan: %w", err), sparkerrors.ExecutionError)
	}

	var schema *StructType
	var allRows []Row

	for {
		response, err := responseClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return allRows, nil
			} else {
				return nil, sparkerrors.WithType(fmt.Errorf("failed to receive plan execution response: %w", err), sparkerrors.ReadError)
			}
		}

		dataType := response.GetSchema()
		if dataType != nil {
			schema, err = convertProtoDataTypeToStructType(dataType)
			if err != nil {
				return nil, err
			}
			continue
		}

		arrowBatch := response.GetArrowBatch()
		if arrowBatch == nil {
			continue
		}

		rowBatch, err := readArrowBatchData(arrowBatch.Data, schema)
		if err != nil {
			return nil, err
		}

		if allRows == nil {
			allRows = make([]Row, 0, len(rowBatch))
		}
		allRows = append(allRows, rowBatch...)
	}
}

func (df *dataFrameImpl) Write() DataFrameWriter {
	return df.Writer()
}

func (df *dataFrameImpl) Writer() DataFrameWriter {
	return newDataFrameWriter(df.sparkSession, df.relation)
}

func (df *dataFrameImpl) CreateTempView(ctx context.Context, viewName string, replace bool, global bool) error {
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_CreateDataframeView{
					CreateDataframeView: &proto.CreateDataFrameViewCommand{
						Input:    df.relation,
						Name:     viewName,
						Replace:  replace,
						IsGlobal: global,
					},
				},
			},
		},
	}

	responseClient, err := df.sparkSession.executePlan(ctx, plan)
	if err != nil {
		return sparkerrors.WithType(fmt.Errorf("failed to create temp view %s: %w", viewName, err), sparkerrors.ExecutionError)
	}

	return responseClient.consumeAll()
}

func (df *dataFrameImpl) Repartition(numPartitions int, columns []string) (DataFrame, error) {
	var partitionExpressions []*proto.Expression
	if columns != nil {
		partitionExpressions = make([]*proto.Expression, 0, len(columns))
		for _, c := range columns {
			expr := &proto.Expression{
				ExprType: &proto.Expression_UnresolvedAttribute_{
					UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
						UnparsedIdentifier: c,
					},
				},
			}
			partitionExpressions = append(partitionExpressions, expr)
		}
	}
	return df.repartitionByExpressions(numPartitions, partitionExpressions)
}

func (df *dataFrameImpl) RepartitionByRange(numPartitions int, columns []RangePartitionColumn) (DataFrame, error) {
	var partitionExpressions []*proto.Expression
	if columns != nil {
		partitionExpressions = make([]*proto.Expression, 0, len(columns))
		for _, c := range columns {
			columnExpr := &proto.Expression{
				ExprType: &proto.Expression_UnresolvedAttribute_{
					UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
						UnparsedIdentifier: c.Name,
					},
				},
			}
			direction := proto.Expression_SortOrder_SORT_DIRECTION_ASCENDING
			if c.Descending {
				direction = proto.Expression_SortOrder_SORT_DIRECTION_DESCENDING
			}
			sortExpr := &proto.Expression{
				ExprType: &proto.Expression_SortOrder_{
					SortOrder: &proto.Expression_SortOrder{
						Child:     columnExpr,
						Direction: direction,
					},
				},
			}
			partitionExpressions = append(partitionExpressions, sortExpr)
		}
	}
	return df.repartitionByExpressions(numPartitions, partitionExpressions)
}

func (df *dataFrameImpl) createPlan() *proto.Plan {
	return &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: df.relation.RelType,
			},
		},
	}
}

func (df *dataFrameImpl) repartitionByExpressions(numPartitions int, partitionExpressions []*proto.Expression) (DataFrame, error) {
	var numPartitionsPointerValue *int32
	if numPartitions != 0 {
		int32Value := int32(numPartitions)
		numPartitionsPointerValue = &int32Value
	}
	df.relation.GetRepartitionByExpression()
	newRelation := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_RepartitionByExpression{
			RepartitionByExpression: &proto.RepartitionByExpression{
				Input:          df.relation,
				NumPartitions:  numPartitionsPointerValue,
				PartitionExprs: partitionExpressions,
			},
		},
	}
	return &dataFrameImpl{
		sparkSession: df.sparkSession,
		relation:     newRelation,
	}, nil
}

func showArrowBatch(arrowBatch *proto.ExecutePlanResponse_ArrowBatch, collector ResultCollector) error {
	return showArrowBatchData(arrowBatch.Data, collector)
}

func showArrowBatchData(data []byte, collector ResultCollector) error {
	rows, err := readArrowBatchData(data, nil)
	if err != nil {
		return err
	}
	for _, row := range rows {
		values, err := row.Values()
		if err != nil {
			return sparkerrors.WithType(fmt.Errorf("failed to get values in the row: %w", err), sparkerrors.ReadError)
		}
		collector.WriteRow(values)
	}
	return nil
}

func readArrowBatchData(data []byte, schema *StructType) ([]Row, error) {
	reader := bytes.NewReader(data)
	arrowReader, err := ipc.NewReader(reader)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to create arrow reader: %w", err), sparkerrors.ReadError)
	}
	defer arrowReader.Release()

	var rows []Row

	for {
		record, err := arrowReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rows, nil
			} else {
				return nil, sparkerrors.WithType(fmt.Errorf("failed to read arrow: %w", err), sparkerrors.ReadError)
			}
		}

		values, err := readArrowRecord(record)
		if err != nil {
			return nil, err
		}

		numRows := int(record.NumRows())
		if rows == nil {
			rows = make([]Row, 0, numRows)
		}

		for _, v := range values {
			row := NewRowWithSchema(v, schema)
			rows = append(rows, row)
		}

		hasNext := arrowReader.Next()
		if !hasNext {
			break
		}
	}

	return rows, nil
}

// readArrowRecordColumn reads all values from arrow record and return [][]any
func readArrowRecord(record arrow.Record) ([][]any, error) {
	numRows := record.NumRows()
	numColumns := int(record.NumCols())

	values := make([][]any, numRows)
	for i := range values {
		values[i] = make([]any, numColumns)
	}

	for columnIndex := 0; columnIndex < numColumns; columnIndex++ {
		err := readArrowRecordColumn(record, columnIndex, values)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

// readArrowRecordColumn reads all values in a column and stores them in values
func readArrowRecordColumn(record arrow.Record, columnIndex int, values [][]any) error {
	numRows := int(record.NumRows())
	columnData := record.Column(columnIndex).Data()
	dataTypeId := columnData.DataType().ID()
	switch dataTypeId {
	case arrow.BOOL:
		vector := array.NewBooleanData(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.INT8:
		vector := array.NewInt8Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.INT16:
		vector := array.NewInt16Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.INT32:
		vector := array.NewInt32Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.INT64:
		vector := array.NewInt64Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.FLOAT16:
		vector := array.NewFloat16Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.FLOAT32:
		vector := array.NewFloat32Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.FLOAT64:
		vector := array.NewFloat64Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.DECIMAL | arrow.DECIMAL128:
		vector := array.NewDecimal128Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.DECIMAL256:
		vector := array.NewDecimal256Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.STRING:
		vector := array.NewStringData(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.BINARY:
		vector := array.NewBinaryData(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.TIMESTAMP:
		vector := array.NewTimestampData(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	case arrow.DATE64:
		vector := array.NewDate64Data(columnData)
		for rowIndex := 0; rowIndex < numRows; rowIndex++ {
			values[rowIndex][columnIndex] = vector.Value(rowIndex)
		}
	default:
		return fmt.Errorf("unsupported arrow data type %s in column %d", dataTypeId.String(), columnIndex)
	}
	return nil
}

func convertProtoDataTypeToStructType(input *proto.DataType) (*StructType, error) {
	dataTypeStruct := input.GetStruct()
	if dataTypeStruct == nil {
		return nil, sparkerrors.WithType(errors.New("dataType.GetStruct() is nil"), sparkerrors.InvalidInputError)
	}
	return &StructType{
		Fields: convertProtoStructFields(dataTypeStruct.Fields),
	}, nil
}

func convertProtoStructFields(input []*proto.DataType_StructField) []StructField {
	result := make([]StructField, len(input))
	for i, f := range input {
		result[i] = convertProtoStructField(f)
	}
	return result
}

func convertProtoStructField(field *proto.DataType_StructField) StructField {
	return StructField{
		Name:     field.Name,
		DataType: convertProtoDataTypeToDataType(field.DataType),
	}
}

// convertProtoDataTypeToDataType converts protobuf data type to Spark connect sql data type
func convertProtoDataTypeToDataType(input *proto.DataType) DataType {
	switch v := input.GetKind().(type) {
	case *proto.DataType_Boolean_:
		return BooleanType{}
	case *proto.DataType_Byte_:
		return ByteType{}
	case *proto.DataType_Short_:
		return ShortType{}
	case *proto.DataType_Integer_:
		return IntegerType{}
	case *proto.DataType_Long_:
		return LongType{}
	case *proto.DataType_Float_:
		return FloatType{}
	case *proto.DataType_Double_:
		return DoubleType{}
	case *proto.DataType_Decimal_:
		return DecimalType{}
	case *proto.DataType_String_:
		return StringType{}
	case *proto.DataType_Binary_:
		return BinaryType{}
	case *proto.DataType_Timestamp_:
		return TimestampType{}
	case *proto.DataType_TimestampNtz:
		return TimestampNtzType{}
	case *proto.DataType_Date_:
		return DateType{}
	default:
		return UnsupportedType{
			TypeInfo: v,
		}
	}
}
