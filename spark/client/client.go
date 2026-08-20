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
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/spark-connect-go/spark/sql/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/apache/spark-connect-go/spark/client/base"
	"github.com/apache/spark-connect-go/spark/mocks"

	"github.com/apache/spark-connect-go/spark/client/options"

	"github.com/google/uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/spark-connect-go/spark/sql/types"

	proto "github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/sparkerrors"
)

type sparkConnectClientImpl struct {
	client    base.SparkConnectRPCClient
	metadata  metadata.MD
	sessionId string
	opts      options.SparkClientOptions
}

func (s *sparkConnectClientImpl) newExecutePlanRequest(plan *proto.Plan) *proto.ExecutePlanRequest {
	// Every new executin needs to get a new operation ID.
	operationId := uuid.NewString()
	return &proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
		UserContext: &proto.UserContext{
			UserId: s.opts.UserId,
		},
		ClientType: &s.opts.UserAgent,
		// Operation ID is needed for being able to reattach.
		OperationId: &operationId,
		RequestOptions: []*proto.ExecutePlanRequest_RequestOption{
			{
				RequestOption: &proto.ExecutePlanRequest_RequestOption_ReattachOptions{
					ReattachOptions: &proto.ReattachOptions{
						Reattachable: s.opts.ReattachExecution,
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
		return nil, nil, nil, sparkerrors.WithType(
			fmt.Errorf("the supplied plan does not contain a command"), sparkerrors.ExecutionError)
	}

	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)
	c, err := s.client.ExecutePlan(ctx, request)
	if err != nil {
		return nil, nil, nil, sparkerrors.WithType(
			fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	respHandler := NewExecuteResponseStream(c, s.sessionId, request, s.opts, s.client, s.metadata)
	schema, table, err := respHandler.ToTable(ctx)
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
		return nil, sparkerrors.WithType(fmt.Errorf(
			"failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	return NewExecuteResponseStream(c, s.sessionId, request, s.opts, s.client, s.metadata), nil
}

// Creates a new AnalyzePlanRequest with the necessary metadata.
func (s *sparkConnectClientImpl) newAnalyzePlanStub() proto.AnalyzePlanRequest {
	return proto.AnalyzePlanRequest{
		SessionId: s.sessionId,
		UserContext: &proto.UserContext{
			UserId: s.opts.UserId,
		},
		ClientType: &s.opts.UserAgent,
	}
}

func (s *sparkConnectClientImpl) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_Schema_{
		Schema: &proto.AnalyzePlanRequest_Schema{
			Plan: plan,
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

func (s *sparkConnectClientImpl) Explain(ctx context.Context, plan *proto.Plan,
	explainMode utils.ExplainMode,
) (*proto.AnalyzePlanResponse, error) {
	var mode proto.AnalyzePlanRequest_Explain_ExplainMode
	switch explainMode {
	case utils.ExplainModeExtended:
		mode = proto.AnalyzePlanRequest_Explain_EXPLAIN_MODE_EXTENDED
	case utils.ExplainModeSimple:
		mode = proto.AnalyzePlanRequest_Explain_EXPLAIN_MODE_SIMPLE
	case utils.ExplainModeCost:
		mode = proto.AnalyzePlanRequest_Explain_EXPLAIN_MODE_COST
	case utils.ExplainModeFormatted:
		mode = proto.AnalyzePlanRequest_Explain_EXPLAIN_MODE_FORMATTED
	case utils.ExplainModeCodegen:
		mode = proto.AnalyzePlanRequest_Explain_EXPLAIN_MODE_CODEGEN
	default:
		return nil, sparkerrors.WithType(fmt.Errorf("unsupported explain mode %v",
			explainMode), sparkerrors.InvalidArgumentError)
	}
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_Explain_{
		Explain: &proto.AnalyzePlanRequest_Explain{
			Plan:        plan,
			ExplainMode: mode,
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

func (s *sparkConnectClientImpl) Persist(ctx context.Context, plan *proto.Plan, storageLevel utils.StorageLevel) error {
	protoLevel := utils.ToProtoStorageLevel(storageLevel)
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_Persist_{
		Persist: &proto.AnalyzePlanRequest_Persist{
			Relation:     plan.GetRoot(),
			StorageLevel: protoLevel,
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	_, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return nil
}

func (s *sparkConnectClientImpl) Unpersist(ctx context.Context, plan *proto.Plan) error {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_Unpersist_{
		Unpersist: &proto.AnalyzePlanRequest_Unpersist{
			Relation: plan.GetRoot(),
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	_, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return nil
}

func (s *sparkConnectClientImpl) GetStorageLevel(ctx context.Context, plan *proto.Plan) (*utils.StorageLevel, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_GetStorageLevel_{
		GetStorageLevel: &proto.AnalyzePlanRequest_GetStorageLevel{
			Relation: plan.GetRoot(),
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return nil, sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}

	level := response.GetGetStorageLevel().StorageLevel
	res := utils.FromProtoStorageLevel(level)
	return &res, nil
}

func (s *sparkConnectClientImpl) SparkVersion(ctx context.Context) (string, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_SparkVersion_{
		SparkVersion: &proto.AnalyzePlanRequest_SparkVersion{},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return "", sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return response.GetSparkVersion().Version, nil
}

func (s *sparkConnectClientImpl) DDLParse(ctx context.Context, sql string) (*types.StructType, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_DdlParse{
		DdlParse: &proto.AnalyzePlanRequest_DDLParse{
			DdlString: sql,
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return nil, sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return types.ConvertProtoDataTypeToStructType(response.GetDdlParse().Parsed)
}

func (s *sparkConnectClientImpl) SameSemantics(ctx context.Context, plan1 *proto.Plan, plan2 *proto.Plan) (bool, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_SameSemantics_{
		SameSemantics: &proto.AnalyzePlanRequest_SameSemantics{
			TargetPlan: plan1,
			OtherPlan:  plan2,
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return false, sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return response.GetSameSemantics().GetResult(), nil
}

func (s *sparkConnectClientImpl) SemanticHash(ctx context.Context, plan *proto.Plan) (int32, error) {
	request := s.newAnalyzePlanStub()
	request.Analyze = &proto.AnalyzePlanRequest_SemanticHash_{
		SemanticHash: &proto.AnalyzePlanRequest_SemanticHash{
			Plan: plan,
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if se := sparkerrors.FromRPCError(err); se != nil {
		return 0, sparkerrors.WithType(se, sparkerrors.ExecutionError)
	}
	return response.GetSemanticHash().GetResult(), nil
}

func (s *sparkConnectClientImpl) Config(ctx context.Context,
	operation *proto.ConfigRequest_Operation,
) (*proto.ConfigResponse, error) {
	request := &proto.ConfigRequest{
		Operation: operation,
		UserContext: &proto.UserContext{
			UserId: s.opts.UserId,
		},
		ClientType: &s.opts.UserAgent,
	}
	request.SessionId = s.sessionId
	resp, err := s.client.Config(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func NewSparkExecutor(conn *grpc.ClientConn, md metadata.MD, sessionId string, opts options.SparkClientOptions) base.SparkConnectClient {
	var client base.SparkConnectRPCClient
	if opts.ReattachExecution {
		client = NewRetriableSparkConnectClient(conn, sessionId, opts)
	} else {
		client = proto.NewSparkConnectServiceClient(conn)
	}
	return &sparkConnectClientImpl{
		client:    client,
		metadata:  md,
		sessionId: sessionId,
		opts:      opts,
	}
}

// NewSparkExecutorFromClient creates a new SparkConnectClient from an existing client and is mostly
// used in testing.
func NewSparkExecutorFromClient(client base.SparkConnectRPCClient, md metadata.MD, sessionId string) base.SparkConnectClient {
	return &sparkConnectClientImpl{
		client:    client,
		metadata:  md,
		sessionId: sessionId,
		opts:      options.DefaultSparkClientOptions,
	}
}

// ExecutePlanClient is the wrapper around the result of the execution of a query plan using
// Spark Connect.
type ExecutePlanClient struct {
	// The GRPC stream to read the response messages.
	responseStream proto.SparkConnectService_ExecutePlanClient
	// The schema of the result of the operation.
	schema *types.StructType
	// The sessionId is ised to verify the server side session.
	sessionId  string
	done       bool
	properties map[string]any
	opts       options.SparkClientOptions

	// The fields below exist to support reattachable execution.
	//
	// When ReattachOptions.Reattachable is set, the server does not hold one
	// response stream open for the lifetime of the query. It ends the stream on
	// its own schedule -- after
	// spark.connect.execute.reattachable.senderMaxStreamDuration, two minutes by
	// default -- and expects the client to resume the same operation with
	// ReattachExecute, quoting the last response it managed to read. Resuming
	// needs the RPC client, the operation's id, and the outgoing metadata the
	// original ExecutePlan call carried, so they travel with the stream.
	client base.SparkConnectRPCClient
	// initialRequest is kept so the execution can be started over if the server
	// has no record of it -- see reattach.
	initialRequest *proto.ExecutePlanRequest
	metadata       metadata.MD
	operationId    string
	lastResponseId string
	// released guards ReleaseExecute(release_all) so the execution is only
	// disowned once.
	released bool
}

func (c *ExecutePlanClient) Properties() map[string]any {
	return c.properties
}

// ToTable converts the result of the execution of a query plan to an Arrow Table.
func (c *ExecutePlanClient) ToTable(ctx context.Context) (*types.StructType, arrow.Table, error) {
	var recordBatches []arrow.Record
	var arrowSchema *arrow.Schema
	recordBatches = make([]arrow.Record, 0)

	// Explicitly needed when tracking re-attachble execution.
	c.done = false
	// Covers every way out of the loop that isn't a completed result: the
	// execution is being abandoned, so tell the server instead of leaving it
	// buffered until it ages out. ResultComplete releases on its own, and
	// releaseAll only ever sends once.
	defer func() {
		if !c.done {
			c.releaseAll()
		}
	}()
	for {
		resp, err := c.responseStream.Recv()
		// EOF is received when the last message has been processed and the stream
		// finished normally.
		if errors.Is(err, io.EOF) {
			// Under reattachable execution EOF does not imply the result is
			// finished -- only ResultComplete does. An EOF before that is the
			// server rotating the stream, and the query is still running, so
			// resume it rather than reporting a truncated result.
			if !c.opts.ReattachExecution || c.done {
				break
			}
			// A rotation that delivered nothing is normal and says nothing about
			// health: a command such as a parquet write emits no responses at all
			// until it finishes, so a query running for many minutes rotates
			// repeatedly with an empty stream each time. Counting those as a
			// stall would cap how long a query is allowed to run. Boundedness
			// comes from ctx instead, as in the reference clients.
			if err := ctx.Err(); err != nil {
				return nil, nil, sparkerrors.WithType(fmt.Errorf(
					"gave up reattaching to operation %s: %w", c.operationId, err), sparkerrors.ExecutionError)
			}
			if err := c.reattach(ctx); err != nil {
				return nil, nil, err
			}
			continue
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

		// Remember where the stream got to. If the server ends it early, this is
		// the point ReattachExecute resumes from, and quoting it is what stops
		// already-delivered batches being replayed into recordBatches.
		if id := resp.GetResponseId(); id != "" {
			c.lastResponseId = id
			// Processed, so the server no longer needs to keep it for a resume.
			c.releaseUntil(id)
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
			// The result is fully delivered, so the execution can be disowned.
			c.releaseAll()
		default:
			// Explicitly ignore messages that we cannot process at the moment.
		}
	}

	// A reattachable execution is only finished once the server has said so with
	// ResultComplete; the loop above resumes on anything short of that, so
	// reaching here without it means the stream ended in a way reattaching
	// cannot recover.
	if c.opts.ReattachExecution && !c.done {
		return nil, nil, sparkerrors.WithType(fmt.Errorf(
			"the result for operation %s is not complete", c.operationId), sparkerrors.ExecutionError)
	}
	// Return the schema and table.
	if arrowSchema == nil {
		return c.schema, nil, nil
	} else {
		return c.schema, array.NewTableFromRecords(arrowSchema, recordBatches), nil
	}
}

// reattach resumes an execution whose response stream the server ended before
// the result was complete, picking up after the last response already seen.
//
// The reattach stream and the original ExecutePlan stream carry the same
// message type, so the resumed stream simply replaces the old one and the
// caller's read loop continues unchanged.
func (c *ExecutePlanClient) reattach(ctx context.Context) error {
	if c.client == nil {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot reattach to operation %s: response stream was created without an RPC client",
			c.operationId), sparkerrors.ExecutionError)
	}

	request := &proto.ReattachExecuteRequest{
		SessionId:   c.sessionId,
		OperationId: c.operationId,
		UserContext: &proto.UserContext{
			UserId: c.opts.UserId,
		},
		ClientType: &c.opts.UserAgent,
	}
	// Omitted on the first reattach of a stream that produced nothing, which
	// tells the server to resume from the beginning rather than after a
	// response it never sent.
	if c.lastResponseId != "" {
		// Copied rather than pointing at the field, which keeps advancing as the
		// resumed stream is read.
		lastResponseId := c.lastResponseId
		request.LastResponseId = &lastResponseId
	}

	stream, err := c.client.ReattachExecute(metadata.NewOutgoingContext(ctx, c.metadata), request)
	if err == nil {
		c.responseStream = stream
		return nil
	}
	if !isUnknownHandle(err) {
		return sparkerrors.WithType(fmt.Errorf(
			"failed to reattach to operation %s in session %s: %w",
			c.operationId, c.sessionId, err), sparkerrors.ExecutionError)
	}

	// The server has no record of the operation, which means the original
	// ExecutePlan never reached it -- so nothing has run and starting over is
	// safe. That only holds while no response has been consumed: once responses
	// are in hand, a fresh execution would replay them and duplicate rows, so
	// fail instead.
	if c.lastResponseId != "" {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s in session %s: responses were already received, "+
				"so re-executing would duplicate them: %w",
			c.operationId, c.sessionId, err), sparkerrors.ExecutionError)
	}
	if c.initialRequest == nil {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s: response stream was created without the originating request: %w",
			c.operationId, err), sparkerrors.ExecutionError)
	}
	restarted, execErr := c.client.ExecutePlan(metadata.NewOutgoingContext(ctx, c.metadata), c.initialRequest)
	if execErr != nil {
		return sparkerrors.WithType(fmt.Errorf(
			"failed to restart operation %s in session %s after the server reported it unknown: %w",
			c.operationId, c.sessionId, execErr), sparkerrors.ExecutionError)
	}
	c.responseStream = restarted
	return nil
}

// unknownHandleMarkers are the server-side error classes meaning the execution
// this client is holding no longer exists (or never did). Matched on the
// message because that is where Spark puts the error class.
var unknownHandleMarkers = []string{
	"INVALID_HANDLE.OPERATION_NOT_FOUND",
	"INVALID_HANDLE.SESSION_NOT_FOUND",
}

func isUnknownHandle(err error) bool {
	if err == nil {
		return false
	}
	for _, marker := range unknownHandleMarkers {
		if strings.Contains(err.Error(), marker) {
			return true
		}
	}
	return false
}

// releaseUntil tells the server it may drop everything it has buffered up to
// and including responseId. Reattachable executions buffer responses so a
// resumed stream can backtrack, and without this the server holds all of them
// until the execution is GC'd.
func (c *ExecutePlanClient) releaseUntil(responseId string) {
	request := c.newReleaseRequest()
	if request == nil {
		return
	}
	request.Release = &proto.ReleaseExecuteRequest_ReleaseUntil_{
		ReleaseUntil: &proto.ReleaseExecuteRequest_ReleaseUntil{ResponseId: responseId},
	}
	c.sendRelease(request)
}

// releaseAll disowns the execution once its result has been consumed, or once
// it has failed in a way no resume can recover. Sent at most once.
func (c *ExecutePlanClient) releaseAll() {
	if c.released {
		return
	}
	c.released = true
	request := c.newReleaseRequest()
	if request == nil {
		return
	}
	request.Release = &proto.ReleaseExecuteRequest_ReleaseAll_{
		ReleaseAll: &proto.ReleaseExecuteRequest_ReleaseAll{},
	}
	c.sendRelease(request)
}

// newReleaseRequest builds the common part of a ReleaseExecute, or returns nil
// when releasing does not apply -- a non-reattachable execution buffers nothing,
// and a stream built without an RPC client has nobody to tell.
func (c *ExecutePlanClient) newReleaseRequest() *proto.ReleaseExecuteRequest {
	if c.client == nil || !c.opts.ReattachExecution {
		return nil
	}
	return &proto.ReleaseExecuteRequest{
		SessionId:   c.sessionId,
		OperationId: c.operationId,
		UserContext: &proto.UserContext{
			UserId: c.opts.UserId,
		},
		ClientType: &c.opts.UserAgent,
	}
}

// sendRelease issues a ReleaseExecute in the background and ignores the outcome.
//
// Deliberately fire-and-forget, matching the reference clients: releasing is an
// optimisation that lets the server reclaim buffered responses sooner, and the
// server already copes with executions that are never released by aging them
// out. Blocking the read loop on it, or failing a query because a release RPC
// did, would both be worse than the buffering it avoids.
func (c *ExecutePlanClient) sendRelease(request *proto.ReleaseExecuteRequest) {
	// Detached from the caller's context: a release issued as the query finishes
	// would otherwise be cancelled the moment the caller returns, which is
	// exactly when it matters most.
	ctx, cancel := context.WithTimeout(
		metadata.NewOutgoingContext(context.Background(), c.metadata), releaseTimeout)
	go func() {
		defer cancel()
		_, _ = c.client.ReleaseExecute(ctx, request)
	}()
}

// releaseTimeout bounds a background ReleaseExecute so a wedged server cannot
// accumulate goroutines for the life of the process.
const releaseTimeout = 30 * time.Second

func NewExecuteResponseStream(
	responseClient proto.SparkConnectService_ExecutePlanClient,
	sessionId string,
	request *proto.ExecutePlanRequest,
	opts options.SparkClientOptions,
	client base.SparkConnectRPCClient,
	md metadata.MD,
) base.ExecuteResponseStream {
	return &ExecutePlanClient{
		responseStream: responseClient,
		sessionId:      sessionId,
		done:           false,
		properties:     make(map[string]any),
		opts:           opts,
		client:         client,
		initialRequest: request,
		metadata:       md,
		operationId:    request.GetOperationId(),
	}
}

// testRequest is the minimal originating request the fixtures need; they run
// with reattachable execution off, so it is never used to restart anything.
func testRequest(sessionId string) *proto.ExecutePlanRequest {
	operationId := uuid.NewString()
	return &proto.ExecutePlanRequest{SessionId: sessionId, OperationId: &operationId}
}

func NewTestConnectClientFromResponses(sessionId string, r ...*mocks.MockResponse) base.SparkConnectClient {
	protoClient := mocks.NewProtoClientMock(r...)
	// No RPC client or metadata: these fixtures run with reattachable execution
	// off, so the stream is never resumed and reattach is unreachable.
	stream := NewExecuteResponseStream(protoClient, sessionId, testRequest(sessionId), options.DefaultSparkClientOptions, nil, nil)
	return &mocks.TestExecutor{
		Client: stream,
	}
}

func NewTestConnectClientWithImmediateError(sessionId string, err error) base.SparkConnectClient {
	stream := NewExecuteResponseStream(nil, sessionId, testRequest(sessionId), options.DefaultSparkClientOptions, nil, nil)
	return &mocks.TestExecutor{
		Client: stream,
		Err:    err,
	}
}
