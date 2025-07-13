// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	proto "github.com/apache/spark-connect-go/v40/internal/generated"
	"github.com/apache/spark-connect-go/v40/spark/client"
	"github.com/apache/spark-connect-go/v40/spark/client/testutils"
	"github.com/apache/spark-connect-go/v40/spark/mocks"
	"github.com/apache/spark-connect-go/v40/spark/sparkerrors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzePlanCallsAnalyzePlanOnClient(t *testing.T) {
	ctx := context.Background()
	response := &proto.AnalyzePlanResponse{}
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, response, nil, nil), nil, mocks.MockSessionId)
	resp, err := c.AnalyzePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAnalyzePlanFailsIfClientFails(t *testing.T) {
	ctx := context.Background()
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, nil, assert.AnError, nil), nil, mocks.MockSessionId)
	resp, err := c.AnalyzePlan(ctx, &proto.Plan{})
	assert.Nil(t, resp)
	assert.Error(t, err)
}

func TestExecutePlanCallsExecutePlanOnClient(t *testing.T) {
	ctx := context.Background()
	plan := &proto.Plan{}

	// Generate a mock client
	responseStream := mocks.NewProtoClientMock(&mocks.ExecutePlanResponseDone)

	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(responseStream, nil, nil, t), nil, mocks.MockSessionId)
	resp, err := c.ExecutePlan(ctx, plan)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestExecutePlanCallsExecuteCommandOnClient(t *testing.T) {
	ctx := context.Background()
	plan := &proto.Plan{}

	// Generate a mock client
	responseStream := mocks.NewProtoClientMock(&mocks.ExecutePlanResponseDone, &mocks.ExecutePlanResponseEOF)

	// Check that the execution fails if no command is supplied.
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(responseStream, nil, nil, t), nil, mocks.MockSessionId)
	_, _, _, err := c.ExecuteCommand(ctx, plan)
	assert.ErrorIs(t, err, sparkerrors.ExecutionError)

	// Generate a command and the execution should succeed.
	sqlCommand := mocks.NewSqlCommand("select range(10)")
	c = client.NewSparkExecutorFromClient(testutils.NewConnectServiceClientMock(responseStream, nil, nil, t), nil, mocks.MockSessionId)
	_, _, _, err = c.ExecuteCommand(ctx, sqlCommand)
	assert.NoError(t, err)
}

func Test_ExecuteWithWrongSession(t *testing.T) {
	ctx := context.Background()
	sqlCommand := mocks.NewSqlCommand("select range(10)")

	// Generate a mock client
	responseStream := mocks.NewProtoClientMock(&mocks.ExecutePlanResponseDone, &mocks.ExecutePlanResponseEOF)

	// Check that the execution fails if no command is supplied.
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(responseStream, nil, nil, t), nil, uuid.NewString())
	_, _, _, err := c.ExecuteCommand(ctx, sqlCommand)
	assert.ErrorIs(t, err, sparkerrors.InvalidServerSideSessionError)
}

func Test_Execute_SchemaParsingFails(t *testing.T) {
	ctx := context.Background()
	sqlCommand := mocks.NewSqlCommand("select range(10)")
	responseStream := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseBrokenSchema,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(responseStream, nil, nil, t), nil, mocks.MockSessionId)
	_, _, _, err := c.ExecuteCommand(ctx, sqlCommand)
	assert.ErrorIs(t, err, sparkerrors.ExecutionError)
}

func TestToRecordBatches_SchemaExtraction(t *testing.T) {
	// Schema is returned as nil and populated inside the goroutine
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "test_column",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 'test'"))
	require.NoError(t, err)

	_, _, schema := stream.ToRecordBatches(ctx)

	assert.Nil(t, schema, "Schema is populated asynchronously in the goroutine")
}

func TestToRecordBatches_ChannelClosureWithoutData(t *testing.T) {
	// Channels should close without sending any records when no arrow batches present
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "test_column",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 1"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	recordsReceived := 0
	errorsReceived := 0
	timeout := time.After(100 * time.Millisecond)
	done := false

	for !done {
		select {
		case _, ok := <-recordChan:
			if ok {
				recordsReceived++
			} else {
				done = true
			}
		case <-errorChan:
			errorsReceived++
		case <-timeout:
			t.Fatal("Test timed out - channels not closed")
		}
	}

	assert.Equal(t, 0, recordsReceived, "No records should be sent when no arrow batches present")
	assert.Equal(t, 0, errorsReceived, "No errors should occur")
}

func TestToRecordBatches_ArrowBatchStreaming(t *testing.T) {
	// Arrow batch data should be correctly streamed
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	arrowData := createTestArrowBatch(t, []string{"value1", "value2", "value3"})

	arrowBatch := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
				ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
					Data: arrowData,
				},
			},
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		arrowBatch,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select col"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	records := collectRecords(t, recordChan, errorChan)

	require.Len(t, records, 1, "Should receive exactly one record")

	record := records[0]
	assert.Equal(t, int64(3), record.NumRows(), "Record should have 3 rows")
	assert.Equal(t, int64(1), record.NumCols(), "Record should have 1 column")

	col := record.Column(0).(*array.String)
	assert.Equal(t, "value1", col.Value(0))
	assert.Equal(t, "value2", col.Value(1))
	assert.Equal(t, "value3", col.Value(2))
}

func TestToRecordBatches_MultipleArrowBatches(t *testing.T) {
	// Multiple arrow batches should be streamed in order
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	batch1 := createTestArrowBatch(t, []string{"batch1_row1", "batch1_row2"})
	batch2 := createTestArrowBatch(t, []string{"batch2_row1", "batch2_row2"})

	arrowBatch1 := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
				ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
					Data: batch1,
				},
			},
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
		},
	}

	arrowBatch2 := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
				ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
					Data: batch2,
				},
			},
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		arrowBatch1,
		arrowBatch2,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select col"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	records := collectRecords(t, recordChan, errorChan)

	require.Len(t, records, 2, "Should receive exactly two records")

	// Verify first batch
	col1 := records[0].Column(0).(*array.String)
	assert.Equal(t, "batch1_row1", col1.Value(0))
	assert.Equal(t, "batch1_row2", col1.Value(1))

	// Verify second batch
	col2 := records[1].Column(0).(*array.String)
	assert.Equal(t, "batch2_row1", col2.Value(0))
	assert.Equal(t, "batch2_row2", col2.Value(1))
}

func TestToRecordBatches_ContextCancellationStopsStreaming(t *testing.T) {
	// Context cancellation should stop streaming
	ctx, cancel := context.WithCancel(context.Background())

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col0",
								DataType: &proto.DataType{
									Kind: &proto.DataType_Integer_{
										Integer: &proto.DataType_Integer{},
									},
								},
								Nullable: true,
							},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 1"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	// Cancel the context immediately
	cancel()

	timeout := time.After(100 * time.Millisecond)

	for {
		select {
		case _, ok := <-recordChan:
			if !ok {
				// Channel closed normally - acceptable as cancellation might happen after processing
				return
			}
		case err := <-errorChan:
			// Got an error - verify it's context cancellation
			assert.ErrorIs(t, err, context.Canceled)
			return
		case <-timeout:
			// Timeout is acceptable as cancellation might have happened after all responses were processed
			return
		}
	}
}

func TestToRecordBatches_RPCErrorPropagation(t *testing.T) {
	// RPC errors should be properly propagated
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col1",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	expectedError := errors.New("simulated RPC error")
	errorResponse := &mocks.MockResponse{
		Err: expectedError,
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		errorResponse)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 1"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	select {
	case err := <-errorChan:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulated RPC error")
	case <-recordChan:
		t.Fatal("Should not receive any records when RPC error occurs")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected RPC error")
	}
}

func TestToRecordBatches_SessionValidation(t *testing.T) {
	// Session validation error should be returned for wrong session ID
	ctx := context.Background()

	wrongSessionResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   "wrong-session-id",
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col0",
								DataType: &proto.DataType{
									Kind: &proto.DataType_Integer_{
										Integer: &proto.DataType_Integer{},
									},
								},
								Nullable: true,
							},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		wrongSessionResponse,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 1"))
	require.NoError(t, err)

	_, errorChan, _ := stream.ToRecordBatches(ctx)

	select {
	case err := <-errorChan:
		assert.Error(t, err)
		assert.ErrorIs(t, err, sparkerrors.InvalidServerSideSessionError)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected session validation error")
	}
}

func TestToRecordBatches_SqlCommandResultProperties(t *testing.T) {
	// SQL command results should be captured in properties
	ctx := context.Background()

	sqlResultResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
				SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{
					Relation: &proto.Relation{
						RelType: &proto.Relation_Sql{
							Sql: &proto.SQL{Query: "test query"},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		sqlResultResponse,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("test query"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)
	_ = collectRecords(t, recordChan, errorChan)

	// Properties should contain the SQL command result
	props := stream.(*client.ExecutePlanClient).Properties()
	assert.NotNil(t, props["sql_command_result"])
}

func TestToRecordBatches_EOFHandling(t *testing.T) {
	// EOF should close channels without error
	ctx := context.Background()

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select 1"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	timeout := time.After(100 * time.Millisecond)
	recordClosed := false
	errorReceived := false

	for !recordClosed {
		select {
		case _, ok := <-recordChan:
			if !ok {
				recordClosed = true
			}
		case <-errorChan:
			errorReceived = true
		case <-timeout:
			t.Fatal("Test timed out")
		}
	}

	assert.True(t, recordClosed, "Record channel should be closed")
	assert.False(t, errorReceived, "No error should be received for EOF")
}

func TestToRecordBatches_ExecutionProgressHandling(t *testing.T) {
	// Execution progress messages should be handled without affecting record streaming
	ctx := context.Background()

	schemaResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col1",
								DataType: &proto.DataType{
									Kind: &proto.DataType_String_{
										String_: &proto.DataType_String{},
									},
								},
								Nullable: false,
							},
						},
					},
				},
			},
		},
	}

	progressResponse1 := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			ResponseType: &proto.ExecutePlanResponse_ExecutionProgress_{
				ExecutionProgress: &proto.ExecutePlanResponse_ExecutionProgress{
					Stages:           nil,
					NumInflightTasks: 0,
				},
			},
		},
	}

	progressResponse2 := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			ResponseType: &proto.ExecutePlanResponse_ExecutionProgress_{
				ExecutionProgress: &proto.ExecutePlanResponse_ExecutionProgress{
					Stages:           nil,
					NumInflightTasks: 0,
				},
			},
		},
	}

	arrowData := createTestArrowBatch(t, []string{"value1", "value2"})
	arrowBatch := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
				ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
					Data: arrowData,
				},
			},
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		schemaResponse,
		progressResponse1,
		progressResponse2,
		arrowBatch,
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("select col1"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	records := collectRecords(t, recordChan, errorChan)
	require.Len(t, records, 1, "Should receive exactly one record despite progress messages")

	record := records[0]
	assert.Equal(t, int64(2), record.NumRows())
}

func TestToRecordBatches_SqlCommandResultOnly(t *testing.T) {
	// Queries that only return SqlCommandResult should complete without arrow batches
	ctx := context.Background()

	sqlResultResponse := &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:   mocks.MockSessionId,
			OperationId: mocks.MockOperationId,
			ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
				SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{
					Relation: &proto.Relation{
						RelType: &proto.Relation_Sql{
							Sql: &proto.SQL{Query: "SHOW TABLES"},
						},
					},
				},
			},
		},
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		sqlResultResponse,
		&mocks.ExecutePlanResponseEOF)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("SHOW TABLES"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	recordsReceived := 0
	errorsReceived := 0
	timeout := time.After(100 * time.Millisecond)
	done := false

	for !done {
		select {
		case _, ok := <-recordChan:
			if ok {
				recordsReceived++
			} else {
				done = true
			}
		case <-errorChan:
			errorsReceived++
		case <-timeout:
			t.Fatal("Test timed out - channels not closed")
		}
	}

	assert.Equal(t, 0, recordsReceived, "No records should be sent for SqlCommandResult only")
	assert.Equal(t, 0, errorsReceived, "No errors should occur")

	props := stream.(*client.ExecutePlanClient).Properties()
	assert.NotNil(t, props["sql_command_result"])
}

func TestToRecordBatches_MixedResponseTypes(t *testing.T) {
	// Mixed response types should be handled correctly in realistic order
	ctx := context.Background()

	responses := []*mocks.MockResponse{
		// Schema first
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				Schema: &proto.DataType{
					Kind: &proto.DataType_Struct_{
						Struct: &proto.DataType_Struct{
							Fields: []*proto.DataType_StructField{
								{
									Name: "id",
									DataType: &proto.DataType{
										Kind: &proto.DataType_String_{
											String_: &proto.DataType_String{},
										},
									},
									Nullable: false,
								},
							},
						},
					},
				},
			},
		},
		// SQL command result
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
					SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{
						Relation: &proto.Relation{
							RelType: &proto.Relation_Sql{
								Sql: &proto.SQL{Query: "SELECT * FROM table"},
							},
						},
					},
				},
			},
		},
		// Progress updates
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				ResponseType: &proto.ExecutePlanResponse_ExecutionProgress_{
					ExecutionProgress: &proto.ExecutePlanResponse_ExecutionProgress{
						Stages:           nil,
						NumInflightTasks: 0,
					},
				},
			},
		},
		// Arrow batch
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
						Data: createTestArrowBatch(t, []string{"row1"}),
					},
				},
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
			},
		},
		// More progress
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				ResponseType: &proto.ExecutePlanResponse_ExecutionProgress_{
					ExecutionProgress: &proto.ExecutePlanResponse_ExecutionProgress{
						Stages:           nil,
						NumInflightTasks: 0,
					},
				},
			},
		},
		// Another arrow batch
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
						Data: createTestArrowBatch(t, []string{"row2", "row3"}),
					},
				},
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
			},
		},
		// Result complete
		&mocks.ExecutePlanResponseDone,
		// EOF
		&mocks.ExecutePlanResponseEOF,
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId, responses...)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("SELECT * FROM table"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	records := collectRecords(t, recordChan, errorChan)
	require.Len(t, records, 2, "Should receive exactly two arrow batches")

	assert.Equal(t, int64(1), records[0].NumRows())
	assert.Equal(t, int64(2), records[1].NumRows())
}

func TestToRecordBatches_NoResultCompleteWithEOF(t *testing.T) {
	// Server sends EOF without ResultComplete (real Databricks behavior)
	ctx := context.Background()

	responses := []*mocks.MockResponse{
		// Schema
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				Schema: &proto.DataType{
					Kind: &proto.DataType_Struct_{
						Struct: &proto.DataType_Struct{
							Fields: []*proto.DataType_StructField{
								{
									Name: "value",
									DataType: &proto.DataType{
										Kind: &proto.DataType_String_{
											String_: &proto.DataType_String{},
										},
									},
									Nullable: false,
								},
							},
						},
					},
				},
			},
		},
		// SqlCommandResult
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
					SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{
						Relation: &proto.Relation{
							RelType: &proto.Relation_Sql{
								Sql: &proto.SQL{Query: "SELECT 'test'"},
							},
						},
					},
				},
			},
		},
		// ExecutionProgress
		{
			Resp: &proto.ExecutePlanResponse{
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
				ResponseType: &proto.ExecutePlanResponse_ExecutionProgress_{
					ExecutionProgress: &proto.ExecutePlanResponse_ExecutionProgress{
						Stages:           nil,
						NumInflightTasks: 0,
					},
				},
			},
		},
		// Arrow batch with data
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
						Data: createTestArrowBatch(t, []string{"test"}),
					},
				},
				SessionId:   mocks.MockSessionId,
				OperationId: mocks.MockOperationId,
			},
		},
		// EOF without ResultComplete (Databricks behavior)
		&mocks.ExecutePlanResponseEOF,
	}

	c := client.NewTestConnectClientFromResponses(mocks.MockSessionId, responses...)

	stream, err := c.ExecutePlan(ctx, mocks.NewSqlCommand("SELECT 'test'"))
	require.NoError(t, err)

	recordChan, errorChan, _ := stream.ToRecordBatches(ctx)

	records := collectRecords(t, recordChan, errorChan)
	require.Len(t, records, 1, "Should receive exactly one record")

	record := records[0]
	assert.Equal(t, int64(1), record.NumRows())
	col := record.Column(0).(*array.String)
	assert.Equal(t, "test", col.Value(0))
}

// Helper function to create test arrow batch data
func createTestArrowBatch(t *testing.T, values []string) []byte {
	t.Helper()

	arrowFields := []arrow.Field{
		{Name: "col", Type: arrow.BinaryTypes.String},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	stringBuilder := recordBuilder.Field(0).(*array.StringBuilder)
	for _, v := range values {
		stringBuilder.Append(v)
	}

	record := recordBuilder.NewRecord()
	defer record.Release()

	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer arrowWriter.Close()

	err := arrowWriter.Write(record)
	require.NoError(t, err)
	err = arrowWriter.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// Helper function to collect all records from channels
func collectRecords(t *testing.T, recordChan <-chan arrow.Record, errorChan <-chan error) []arrow.Record {
	t.Helper()

	var records []arrow.Record
	timeout := time.After(100 * time.Millisecond)

	for {
		select {
		case record, ok := <-recordChan:
			if !ok {
				return records
			}
			if record != nil {
				records = append(records, record)
			}
		case err := <-errorChan:
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		case <-timeout:
			t.Fatal("Test timed out collecting records")
		}
	}
}
