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
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/spark-connect-go/v35/spark/client/base"

	"github.com/apache/spark-connect-go/v35/spark/client/options"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/client"
	"github.com/apache/spark-connect-go/v35/spark/client/channel"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type SparkSession interface {
	Read() DataFrameReader
	Sql(ctx context.Context, query string) (DataFrame, error)
	Stop() error
	Table(name string) (DataFrame, error)
	CreateDataFrameFromArrow(ctx context.Context, data arrow.Table) (DataFrame, error)
	CreateDataFrame(ctx context.Context, data [][]any, schema *types.StructType) (DataFrame, error)
}

// NewSessionBuilder creates a new session builder for starting a new spark session
func NewSessionBuilder() *SparkSessionBuilder {
	return &SparkSessionBuilder{}
}

type SparkSessionBuilder struct {
	connectionString string
	channelBuilder   channel.Builder
}

// Remote sets the connection string for remote connection
func (s *SparkSessionBuilder) Remote(connectionString string) *SparkSessionBuilder {
	s.connectionString = connectionString
	return s
}

func (s *SparkSessionBuilder) WithChannelBuilder(cb channel.Builder) *SparkSessionBuilder {
	s.channelBuilder = cb
	return s
}

func (s *SparkSessionBuilder) Build(ctx context.Context) (SparkSession, error) {
	if s.channelBuilder == nil {
		cb, err := channel.NewBuilder(s.connectionString)
		if err != nil {
			return nil, sparkerrors.WithType(fmt.Errorf(
				"failed to connect to remote %s: %w", s.connectionString, err), sparkerrors.ConnectionError)
		}
		s.channelBuilder = cb
	}
	conn, err := s.channelBuilder.Build(ctx)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w",
			s.connectionString, err), sparkerrors.ConnectionError)
	}

	// Add metadata to the request.
	meta := metadata.MD{}
	for k, v := range s.channelBuilder.Headers() {
		meta[k] = append(meta[k], v)
	}

	sessionId := uuid.NewString()
	return &sparkSessionImpl{
		sessionId: sessionId,
		client:    client.NewSparkExecutor(conn, meta, sessionId, options.DefaultSparkClientOptions),
	}, nil
}

type sparkSessionImpl struct {
	sessionId string
	client    base.SparkConnectClient
}

func (s *sparkSessionImpl) Read() DataFrameReader {
	return NewDataframeReader(s)
}

// Sql executes a sql query and returns the result as a DataFrame
func (s *sparkSessionImpl) Sql(ctx context.Context, query string) (DataFrame, error) {
	// Due to the nature of Spark, we have to first submit the SQL query immediately as a command
	// to make sure that all side effects have been executed properly. If no side effects are present,
	// then simply prepare this as a SQL relation.

	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_SqlCommand{
					SqlCommand: &proto.SqlCommand{
						Sql: query,
					},
				},
			},
		},
	}
	// We need an execute command here.
	_, _, properties, err := s.client.ExecuteCommand(ctx, plan)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to execute sql: %s: %w", query, err), sparkerrors.ExecutionError)
	}

	val, ok := properties["sql_command_result"]
	if !ok {
		plan := &proto.Relation{
			Common: &proto.RelationCommon{
				PlanId: newPlanId(),
			},
			RelType: &proto.Relation_Sql{
				Sql: &proto.SQL{
					Query: query,
				},
			},
		}
		return NewDataFrame(s, plan), nil
	} else {
		rel := val.(*proto.Relation)
		rel.Common = &proto.RelationCommon{
			PlanId: newPlanId(),
		}
		return NewDataFrame(s, rel), nil
	}
}

func (s *sparkSessionImpl) Stop() error {
	return nil
}

func (s *sparkSessionImpl) Table(name string) (DataFrame, error) {
	return s.Read().Table(name)
}

func (s *sparkSessionImpl) CreateDataFrameFromArrow(ctx context.Context, data arrow.Table) (DataFrame, error) {
	// Generate the schema.
	// schema := types.ArrowSchemaToProto(data.Schema())
	// schemaString := ""
	// TODO (PySpark does a lot of casting here to convert the schema that does not exist yet.

	// Convert the Arrow Table into a byte array of arrow IPC messages.
	buf := new(bytes.Buffer)
	w := ipc.NewWriter(buf, ipc.WithSchema(data.Schema()))
	defer w.Close()

	// Create a RecordReader from the table
	rr := array.NewTableReader(data, int64(data.NumRows()))
	defer rr.Release()

	// Read the records from the table and write them to the buffer
	for rr.Next() {
		record := rr.Record()
		if err := w.Write(record); err != nil {
			return nil, sparkerrors.WithType(fmt.Errorf("failed to write record: %w", err), sparkerrors.WriteError)
		}
	}

	// Create a local relation object
	plan := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_LocalRelation{
			LocalRelation: &proto.LocalRelation{
				// Schema: &schemaString,
				Data: buf.Bytes(),
			},
		},
	}

	// Capture the column names from the schema:
	columnNames := make([]string, data.NumCols())
	for i, field := range data.Schema().Fields() {
		columnNames[i] = field.Name
	}

	dfPlan := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_ToDf{
			ToDf: &proto.ToDF{
				Input:       plan,
				ColumnNames: columnNames,
			},
		},
	}
	return NewDataFrame(s, dfPlan), nil
}

func (s *sparkSessionImpl) CreateDataFrame(ctx context.Context, data [][]any, schema *types.StructType) (DataFrame, error) {
	pool := memory.NewGoAllocator()
	// Convert the data into an Arrow Table
	arrowSchema := arrow.NewSchema(schema.ToArrowType().Fields(), nil)
	rb := array.NewRecordBuilder(pool, arrowSchema)
	defer rb.Release()
	// Iterate over all fields and add the values:
	for _, row := range data {
		for i, field := range schema.Fields {
			switch field.DataType {
			case types.BOOLEAN:
				rb.Field(i).(*array.BooleanBuilder).Append(row[i].(bool))
			case types.BYTE:
				rb.Field(i).(*array.Int8Builder).Append(int8(row[i].(int)))
			case types.SHORT:
				rb.Field(i).(*array.Int16Builder).Append(int16(row[i].(int)))
			case types.INTEGER:
				rb.Field(i).(*array.Int32Builder).Append(int32(row[i].(int)))
			case types.LONG:
				rb.Field(i).(*array.Int64Builder).Append(int64(row[i].(int)))
			case types.FLOAT:
				rb.Field(i).(*array.Float32Builder).Append(float32(row[i].(float32)))
			case types.DOUBLE:
				rb.Field(i).(*array.Float64Builder).Append(row[i].(float64))
			case types.STRING:
				rb.Field(i).(*array.StringBuilder).Append(row[i].(string))
			case types.DATE:
				rb.Field(i).(*array.Date32Builder).Append(
					arrow.Date32FromTime(row[i].(time.Time)))
			case types.TIMESTAMP:
				ts, err := arrow.TimestampFromTime(row[i].(time.Time), arrow.Millisecond)
				if err != nil {
					return nil, err
				}
				rb.Field(i).(*array.TimestampBuilder).Append(ts)
			default:
				return nil, sparkerrors.WithType(fmt.Errorf(
					"unsupported data type: %s", field.DataType), sparkerrors.NotImplementedError)
			}
		}
	}
	rec := rb.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer tbl.Release()
	return s.CreateDataFrameFromArrow(ctx, tbl)
}
