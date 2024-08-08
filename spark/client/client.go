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

package client

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"

	"github.com/apache/spark-connect-go/v35/spark/client/base"
	"github.com/apache/spark-connect-go/v35/spark/mocks"

	"github.com/apache/spark-connect-go/v35/spark/client/options"

	"github.com/google/uuid"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	"github.com/apache/spark-connect-go/v35/internal/generated"
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

type sparkConnectClientImpl struct {
	client              base.SparkConnectRPCClient
	metadata            metadata.MD
	sessionId           string
	reattachableRequest bool
}

func (s *sparkConnectClientImpl) newExecutePlanRequest(plan *proto.Plan) *proto.ExecutePlanRequest {
	// Every new executin needs to get a new operation ID.
	operationId := uuid.NewString()
	return &proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
		// Operation ID is needed for being able to reattach.
		OperationId: &operationId,
		RequestOptions: []*proto.ExecutePlanRequest_RequestOption{
			{
				RequestOption: &proto.ExecutePlanRequest_RequestOption_ReattachOptions{
					ReattachOptions: &proto.ReattachOptions{
						Reattachable: s.reattachableRequest,
					},
				},
			},
		},
	}
}

func (s *sparkConnectClientImpl) ExecuteCommand(ctx context.Context, plan *proto.Plan) (arrow.Table, *types.StructType, map[string]any, error) {
	request := s.newExecutePlanRequest(plan)

	// Check that the supplied plan is actually a command.
	if plan.GetCommand() == nil {
		return nil, nil, nil, sparkerrors.WithType(fmt.Errorf("the supplied plan does not contain a command"), sparkerrors.ExecutionError)
	}

	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)
	c, err := s.client.ExecutePlan(ctx, request)
	if err != nil {
		return nil, nil, nil, sparkerrors.WithType(fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	respHandler := NewExecuteResponseStream(c, s.sessionId, *request.OperationId)
	schema, table, err := respHandler.ToTable()
	if err != nil {
		return nil, nil, nil, err
	}
	return table, schema, respHandler.Properties(), nil
}

func (s *sparkConnectClientImpl) ExecutePlan(ctx context.Context, plan *proto.Plan) (base.ExecuteResponseStream, error) {
	request := s.newExecutePlanRequest(plan)

	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)
	c, err := s.client.ExecutePlan(ctx, request)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	return NewExecuteResponseStream(c, s.sessionId, *request.OperationId), nil
}

func (s *sparkConnectClientImpl) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	request := proto.AnalyzePlanRequest{
		SessionId: s.sessionId,
		Analyze: &proto.AnalyzePlanRequest_Schema_{
			Schema: &proto.AnalyzePlanRequest_Schema{
				Plan: plan,
			},
		},
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return nil, sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return response, nil
}

func NewSparkExecutor(conn *grpc.ClientConn, md metadata.MD, sessionId string, opts options.SparkClientOptions) base.SparkConnectClient {
	var client base.SparkConnectRPCClient
	if opts.ReattachExecution {
		client = NewRetriableSparkConnectClient(conn, sessionId, opts)
	} else {
		client = generated.NewSparkConnectServiceClient(conn)
	}
	return &sparkConnectClientImpl{
		client:              client,
		metadata:            md,
		sessionId:           sessionId,
		reattachableRequest: opts.ReattachExecution,
	}
}

// NewSparkExecutorFromClient creates a new SparkConnectClient from an existing client and is mostly
// used in testing.
func NewSparkExecutorFromClient(client base.SparkConnectRPCClient, md metadata.MD, sessionId string) base.SparkConnectClient {
	return &sparkConnectClientImpl{
		client:              client,
		metadata:            md,
		sessionId:           sessionId,
		reattachableRequest: options.DefaultSparkClientOptions.ReattachExecution,
	}
}

// ExecutePlanClient is the wrapper around the result of the execution of a query plan using
// Spark Connect.
type ExecutePlanClient struct {
	// The GRPC stream to read the response messages.
	responseStream generated.SparkConnectService_ExecutePlanClient
	// The schema of the result of the operation.
	schema *types.StructType
	// The sessionId is ised to verify the server side session.
	sessionId  string
	done       bool
	properties map[string]any
}

func (c *ExecutePlanClient) Properties() map[string]any {
	return c.properties
}

// ToTable converts the result of the execution of a query plan to an Arrow Table.
func (c *ExecutePlanClient) ToTable() (*types.StructType, arrow.Table, error) {
	var recordBatches []arrow.Record
	var arrowSchema *arrow.Schema
	recordBatches = make([]arrow.Record, 0)

	for {
		resp, err := c.responseStream.Recv()
		// EOF is received when the last message has been processed and the stream
		// finished normally.
		if err == io.EOF {
			break
		}

		// If the error was not EOF, there might be another error.
		if se := sparkerrors.FromRPCError(err); se != nil {
			return nil, nil, sparkerrors.WithType(se, sparkerrors.ExecutionError)
		}

		// Process the message

		// Check that the server returned the session ID that we were expecting
		// and that it has not changed.
		if resp.GetSessionId() != c.sessionId {
			return c.schema, nil, sparkerrors.WithType(&sparkerrors.InvalidServerSideSessionDetailsError{
				OwnSessionId:      c.sessionId,
				ReceivedSessionId: resp.GetSessionId(),
			}, sparkerrors.InvalidServerSideSessionError)
		}

		// Check if the response has already the schema set and if yes, convert
		// the proto DataType to a StructType.
		if resp.Schema != nil {
			c.schema, err = types.ConvertProtoDataTypeToStructType(resp.Schema)
			if err != nil {
				return nil, nil, sparkerrors.WithType(err, sparkerrors.ExecutionError)
			}
		}

		switch x := resp.ResponseType.(type) {
		case *proto.ExecutePlanResponse_SqlCommandResult_:
			if val := x.SqlCommandResult.GetRelation(); val != nil {
				c.properties["sql_command_result"] = val
			}
		case *proto.ExecutePlanResponse_ArrowBatch_:
			// Do nothing.
			record, err := types.ReadArrowBatchToRecord(x.ArrowBatch.Data, c.schema)
			if err != nil {
				return nil, nil, err
			}
			arrowSchema = record.Schema()
			record.Retain()
			recordBatches = append(recordBatches, record)
		case *proto.ExecutePlanResponse_ResultComplete_:
			c.done = true
		default:
			// Explicitly ignore messages that we cannot process at the moment.
		}
	}

	// Check that the result is logically complete. The result might not be complete
	// because after 2 minutes the server will interrupt the connection and we have to
	// send a ReAttach execute request.
	//if !c.done {
	//	return nil, nil, sparkerrors.WithType(fmt.Errorf("the result is not complete"), sparkerrors.ExecutionError)
	//}
	// Return the schema and table.
	if arrowSchema == nil {
		return c.schema, nil, nil
	} else {
		return c.schema, array.NewTableFromRecords(arrowSchema, recordBatches), nil
	}
}

func NewExecuteResponseStream(
	responseClient proto.SparkConnectService_ExecutePlanClient,
	sessionId string,
	operationId string,
) base.ExecuteResponseStream {
	return &ExecutePlanClient{
		responseStream: responseClient,
		sessionId:      sessionId,
		done:           false,
		properties:     make(map[string]any),
	}
}

func NewTestConnectClientFromResponses(sessionId string, r ...*mocks.MockResponse) base.SparkConnectClient {
	protoClient := mocks.NewProtoClientMock(r...)
	stream := NewExecuteResponseStream(protoClient, sessionId, uuid.NewString())
	return &mocks.TestExecutor{
		Client: stream,
	}
}

func NewTestConnectClientWithImmediateError(sessionId string, err error) base.SparkConnectClient {
	stream := NewExecuteResponseStream(nil, sessionId, uuid.NewString())
	return &mocks.TestExecutor{
		Client: stream,
		Err:    err,
	}
}
