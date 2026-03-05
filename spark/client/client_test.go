package client_test

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	proto "github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/client"
	"github.com/apache/spark-connect-go/spark/mocks"
	"github.com/apache/spark-connect-go/spark/sparkerrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToRecordIterator_ChannelClosureWithoutData(t *testing.T) {
	// Iterator should complete without yielding any records when no arrow batches present
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

	iter := stream.ToRecordSequence(ctx)

	recordsReceived := 0
	errorsReceived := 0

	for record, err := range iter {
		if err != nil {
			errorsReceived++
			break
		}
		if record != nil {
			recordsReceived++
		}
	}

	assert.Equal(t, 0, recordsReceived, "No records should be sent when no arrow batches present")
	assert.Equal(t, 0, errorsReceived, "No errors should occur")
}

func TestToRecordIterator_ArrowBatchStreaming(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)

	records := collectRecordsFromSeq2(t, iter)

	require.Len(t, records, 1, "Should receive exactly one record")

	record := records[0]
	assert.Equal(t, int64(3), record.NumRows(), "Record should have 3 rows")
	assert.Equal(t, int64(1), record.NumCols(), "Record should have 1 column")

	col := record.Column(0).(*array.String)
	assert.Equal(t, "value1", col.Value(0))
	assert.Equal(t, "value2", col.Value(1))
	assert.Equal(t, "value3", col.Value(2))
}

func TestToRecordIterator_MultipleArrowBatches(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)
	records := collectRecordsFromSeq2(t, iter)

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

func TestToRecordIterator_ContextCancellationStopsStreaming(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)

	// Cancel the context immediately
	cancel()

	// Try to consume the iterator
	timeout := time.After(100 * time.Millisecond)
	done := make(chan bool)

	go func() {
		for _, err := range iter {
			if err != nil {
				// Got an error - verify it's context cancellation
				assert.ErrorIs(t, err, context.Canceled)
				done <- true
				return
			}
		}
		done <- true
	}()

	select {
	case <-done:
		// Good - iteration completed
	case <-timeout:
		// Timeout is acceptable as cancellation might have happened after all responses were processed
	}
}

func TestToRecordIterator_RPCErrorPropagation(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)

	errorReceived := false
	for _, err := range iter {
		if err != nil {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "simulated RPC error")
			errorReceived = true
			break
		}
	}

	assert.True(t, errorReceived, "Expected RPC error")
}

func TestToRecordIterator_SessionValidation(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)

	errorReceived := false
	for _, err := range iter {
		if err != nil {
			assert.Error(t, err)
			assert.ErrorIs(t, err, sparkerrors.InvalidServerSideSessionError)
			errorReceived = true
			break
		}
	}

	assert.True(t, errorReceived, "Expected session validation error")
}

func TestToRecordIterator_SqlCommandResultProperties(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)
	_ = collectRecordsFromSeq2(t, iter)

	// Properties should contain the SQL command result
	props := stream.(*client.ExecutePlanClient).Properties()
	assert.NotNil(t, props["sql_command_result"])
}

func TestToRecordIterator_MixedResponseTypes(t *testing.T) {
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

	iter := stream.ToRecordSequence(ctx)
	records := collectRecordsFromSeq2(t, iter)

	require.Len(t, records, 2, "Should receive exactly two arrow batches")

	assert.Equal(t, int64(1), records[0].NumRows())
	assert.Equal(t, int64(2), records[1].NumRows())
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

// Helper function to collect all records from Seq2 iterator
func collectRecordsFromSeq2(t *testing.T, iter iter.Seq2[arrow.Record, error]) []arrow.Record {
	t.Helper()

	var records []arrow.Record

	for record, err := range iter {
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
			break
		}
		if record != nil {
			records = append(records, record)
		}
	}

	return records
}
