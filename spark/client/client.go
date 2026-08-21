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
	// restarted guards re-running initialRequest, so an execution the server has
	// lost is only ever assumed lost in transit once -- see restart.
	restarted bool
	// retryPolicies decide whether a response stream that broke rather than ended
	// is worth resuming, and how often. The same policies the retrying RPC client
	// applies to a fresh stream, so the two layers agree on what counts as
	// transient and on how long to keep trying.
	retryPolicies []RetryPolicy
	// streamErrors is the retry budget spent resuming a stream that broke. Reset
	// by every response that arrives, so it bounds consecutive failures rather
	// than the lifetime of a query that is making progress.
	streamErrors retryState
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
	// Both belong to one attempt at reading a result, unlike restarted and
	// lastResponseId, which describe the execution itself and have to survive.
	c.streamErrors = retryState{}
	// ReadArrowBatchToRecord hands back a record this function owns, and
	// NewTableFromRecords takes its own references to the arrays underneath, so
	// the reference held here is always ours to drop -- on the way out with a
	// finished table just as much as on the way out with an error. Without this a
	// query leaks its entire result.
	defer func() {
		for _, record := range recordBatches {
			record.Release()
		}
	}()
	// Covers every way out of the loop that isn't a completed result: the
	// execution is being abandoned, so tell the server instead of leaving it
	// buffered until it ages out. ResultComplete releases on its own, and
	// releaseAll only ever sends once.
	defer func() {
		if !c.done {
			c.releaseAll()
		}
	}()
readLoop:
	for {
		resp, err := c.responseStream.Recv()
		// EOF is received when the last message has been processed and the stream
		// finished normally.
		if errors.Is(err, io.EOF) {
			// Under reattachable execution EOF does not imply the result is
			// finished -- only ResultComplete does. An EOF before that is the
			// server rotating the stream, and the query is still running, so
			// resume it rather than reporting a truncated result.
			if !c.opts.ReattachExecution {
				break
			}
			if err := c.resume(ctx); err != nil {
				return nil, nil, err
			}
			continue
		}

		// If the error was not EOF, there might be another error.
		if se := sparkerrors.FromRPCError(err); se != nil {
			// A stream that breaks mid-flight is recoverable for the same reason
			// one the server ends cleanly is: the operation keeps running server
			// side and ReattachExecute picks it up from the last response seen.
			// Only the very first stream gets this from the retrying RPC client;
			// a resumed stream is a plain one, so handling it here is what keeps
			// a long query as resilient after its first rotation as before it.
			policy, transient := c.resumePolicyFor(err)
			recoverable := c.opts.ReattachExecution && transient
			if !recoverable {
				return nil, nil, sparkerrors.WithType(se, sparkerrors.ExecutionError)
			}
			if err := c.backOffBeforeResume(ctx, policy, err); err != nil {
				return nil, nil, err
			}
			if err := c.resume(ctx); err != nil {
				return nil, nil, err
			}
			continue
		}

		// A response arrived, so whatever went wrong on an earlier stream is not
		// what is happening now and the budget above starts over.
		c.streamErrors = retryState{}

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
			recordBatches = append(recordBatches, record)
		case *proto.ExecutePlanResponse_ResultComplete_:
			c.done = true
			// The result is fully delivered, so the execution can be disowned.
			c.releaseAll()
			// A reattachable execution ends here rather than on the EOF that
			// follows: it has just been disowned, so if that extra read returned
			// an error instead it would fail a query whose result is complete and
			// no longer recoverable. Nothing is disowned when reattaching is off
			// -- and a server only sends ResultComplete when it is on -- so that
			// path keeps reading to EOF as before.
			if c.opts.ReattachExecution {
				break readLoop
			}
		default:
			// Explicitly ignore messages that we cannot process at the moment.
		}
	}

	// Return the schema and table.
	if arrowSchema == nil {
		return c.schema, nil, nil
	}
	return c.schema, array.NewTableFromRecords(arrowSchema, recordBatches), nil
}

// resume picks an execution back up after the stream it was being read from
// stopped producing, whether the server ended it cleanly or it broke.
//
// After a clean rotation the caller's ctx is the only bound on how often this
// happens, deliberately: a rotation that delivered nothing is normal and says
// nothing about health, since a command such as a parquet write emits no
// responses at all until it finishes, so a query running for many minutes
// rotates repeatedly with an empty stream each time. Counting those as a stall
// would cap how long a query may run. A rotation that broke rather than ended is
// bounded as well -- see backOffBeforeResume, which the caller applies first.
func (c *ExecutePlanClient) resume(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return sparkerrors.WithType(fmt.Errorf(
			"gave up reattaching to operation %s: %w", c.operationId, err), sparkerrors.ExecutionError)
	}
	return c.reattach(ctx)
}

// resumePolicyFor returns the policy that considers a broken response stream
// worth resuming, if any of them does. Its budget and backoff govern how long
// resuming keeps being attempted, so classification and patience come from the
// same place the retrying RPC client takes them from.
func (c *ExecutePlanClient) resumePolicyFor(err error) (RetryPolicy, bool) {
	for _, policy := range c.retryPolicies {
		if policy.Handler(err) {
			return policy, true
		}
	}
	return RetryPolicy{}, false
}

// backOffBeforeResume spends one attempt from the budget and waits out its
// backoff, or reports that resuming has stopped being worth trying.
//
// The EOF path needs neither, which is why only this one has them: the server
// paces a clean rotation by holding each stream open for
// senderMaxStreamDuration, so resuming has already cost a stream lifetime. A
// stream that fails on its first Recv costs nothing, so without a budget and a
// wait here a server failing every resumed stream would be reattached to in a
// tight loop for as long as the caller's context allowed -- which for a caller
// passing context.Background() is forever.
func (c *ExecutePlanClient) backOffBeforeResume(ctx context.Context, policy RetryPolicy, cause error) error {
	wait := c.streamErrors.nextAttempt(policy)
	if wait == nil {
		return sparkerrors.WithType(fmt.Errorf(
			"gave up resuming operation %s in session %s: %d consecutive attempts failed without "+
				"the stream delivering anything: %w",
			c.operationId, c.sessionId, policy.MaxRetries, cause), sparkerrors.RetriesExceeded)
	}
	timer := time.NewTimer(*wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return sparkerrors.WithType(fmt.Errorf(
			"gave up reattaching to operation %s: %w", c.operationId, ctx.Err()), sparkerrors.ExecutionError)
	case <-timer.C:
		return nil
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
	// A lost session is deliberately not treated like a lost operation, which is
	// where this client diverges from the reference ones: Scala's
	// ExecutePlanResponseReattachableIterator and PySpark's reattach.py match both
	// classes in one branch and re-issue the original ExecutePlan.
	//
	// What makes that safe for them is a check this client does not have yet. Both
	// track the server's own session id across every response of a session --
	// ResponseValidator.verifyResponse and _verify_response_integrity -- so a
	// replay that lands in a session the server rebuilt is caught on its first
	// response and fails loudly. Here initialRequest pins the original session id,
	// the server creates a fresh empty session under it, and nothing would notice:
	// the query would return a result computed without the temporary views, cached
	// tables and SQL confs the caller set up. Refusing is the honest answer until
	// that validation exists, at which point this can move to restart and match
	// the reference exactly. See the commented-out serverSideSessionId in retry.go.
	if isSessionNotFound(err) {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot reattach to operation %s: session %s no longer exists on the server, so the "+
				"state this query was written against is gone -- create a new session to reconnect: %w",
			c.operationId, c.sessionId, err), sparkerrors.ExecutionError)
	}
	if !isOperationNotFound(err) {
		return sparkerrors.WithType(fmt.Errorf(
			"failed to reattach to operation %s in session %s: %w",
			c.operationId, c.sessionId, err), sparkerrors.ExecutionError)
	}
	return c.restart(ctx, err)
}

// restart runs the original ExecutePlan again after the server reported it has
// no record of the operation.
//
// OPERATION_NOT_FOUND does not distinguish the two things that produce it: the
// original ExecutePlan never reached the server, or it did, ran, and the
// execution was dropped afterwards. Replaying is only safe where telling those
// apart does not matter, so every case where it does is refused here instead.
func (c *ExecutePlanClient) restart(ctx context.Context, cause error) error {
	if c.initialRequest == nil {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s: response stream was created without the originating request: %w",
			c.operationId, cause), sparkerrors.ExecutionError)
	}
	// Responses already in hand would be replayed by a fresh execution and
	// silently duplicate rows.
	if c.lastResponseId != "" {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s in session %s: responses were already received, "+
				"so re-executing would duplicate them: %w",
			c.operationId, c.sessionId, cause), sparkerrors.ExecutionError)
	}
	// Having received nothing is not evidence that nothing ran: a command such as
	// a parquet write emits no responses at all until it finishes, so a write the
	// server did execute and then forgot looks exactly like one it never
	// received, and re-running it would write the data twice.
	//
	// Being rooted in a relation is not enough to rule that out -- see
	// replayableRelation for why.
	if !replayableRelation(c.initialRequest.GetPlan().GetRoot()) {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s in session %s: %s is not known to be free of side effects, "+
				"so replaying it could repeat something the server has already done: %w",
			c.operationId, c.sessionId, planShape(c.initialRequest.GetPlan()), cause),
			sparkerrors.ExecutionError)
	}
	// Once only. A second execution the server has no record of is not a request
	// that failed to arrive; it is something retrying will not fix.
	if c.restarted {
		return sparkerrors.WithType(fmt.Errorf(
			"cannot restart operation %s in session %s again: it was restarted once already "+
				"and the server lost it a second time: %w",
			c.operationId, c.sessionId, cause), sparkerrors.ExecutionError)
	}
	restarted, err := c.client.ExecutePlan(metadata.NewOutgoingContext(ctx, c.metadata), c.initialRequest)
	if err != nil {
		return sparkerrors.WithType(fmt.Errorf(
			"failed to restart operation %s in session %s after the server reported it unknown: %w",
			c.operationId, c.sessionId, err), sparkerrors.ExecutionError)
	}
	c.restarted = true
	c.responseStream = restarted
	return nil
}

// planShape names what a plan is rooted in, so the refusal above can say which
// shape it saw. Without it the message is unactionable in the one case that
// matters: replayableRelation refuses anything it has not been taught about, so a
// maintainer who adds a dataframe operation and forgets to list it reads "not
// known to be free of side effects" about a Project and has nothing to go on.
// Naming the type separates a deliberate refusal from a missing case.
func planShape(plan *proto.Plan) string {
	switch {
	case plan.GetRoot().GetRelType() != nil:
		return fmt.Sprintf("%T", plan.GetRoot().GetRelType())
	case plan.GetCommand().GetCommandType() != nil:
		return fmt.Sprintf("%T", plan.GetCommand().GetCommandType())
	default:
		return "a plan with nothing set"
	}
}

// replayableRelation reports whether a relation is the dataframe algebra this
// client composes itself, and so can be executed a second time without
// repeating anything the first attempt may already have done.
//
// A list of what is safe rather than a list of what is dangerous, because the
// guard it serves has to refuse whatever it cannot vouch for. Being rooted in a
// relation proves very little on its own: Relation_Sql hands the server raw SQL
// text that may be an INSERT, Relation_Catalog carries CreateTable and
// DropTempView, the map and group variants run user code the server cannot
// characterise, and a relation type added to the proto after this was written is
// unknown by definition. Every one of those is a way to change server state
// through a plan that GetRoot() reports as a relation. Listing the safe cases
// means each of them, and each one added later, is refused until someone has
// looked at it -- and the cost of being wrong that way is a query that reports a
// lost execution instead of silently redoing it.
func replayableRelation(root *proto.Relation) bool {
	switch root.GetRelType().(type) {
	case *proto.Relation_Read, *proto.Relation_Range, *proto.Relation_Project,
		*proto.Relation_Filter, *proto.Relation_Join, *proto.Relation_SetOp,
		*proto.Relation_Sort, *proto.Relation_Limit, *proto.Relation_Offset,
		*proto.Relation_Tail,
		*proto.Relation_Aggregate, *proto.Relation_Sample, *proto.Relation_Deduplicate,
		*proto.Relation_Repartition, *proto.Relation_RepartitionByExpression,
		*proto.Relation_SubqueryAlias, *proto.Relation_ToDf, *proto.Relation_LocalRelation,
		*proto.Relation_WithColumns, *proto.Relation_WithColumnsRenamed,
		*proto.Relation_WithWatermark, *proto.Relation_Drop, *proto.Relation_DropNa,
		*proto.Relation_FillNa, *proto.Relation_Replace, *proto.Relation_Unpivot,
		*proto.Relation_Summary, *proto.Relation_Describe, *proto.Relation_Crosstab,
		*proto.Relation_Cov, *proto.Relation_Corr, *proto.Relation_ApproxQuantile,
		*proto.Relation_FreqItems, *proto.Relation_ShowString:
		return true
	default:
		return false
	}
}

// The server-side error classes meaning the handle this client is holding no
// longer exists (or never did). Matched on the message because that is where
// Spark puts the error class. They are kept apart because only a lost operation
// can be started over here -- see reattach for why a lost session cannot, and
// why the reference clients can.
const (
	operationNotFoundMarker = "INVALID_HANDLE.OPERATION_NOT_FOUND"
	sessionNotFoundMarker   = "INVALID_HANDLE.SESSION_NOT_FOUND"
)

func isOperationNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), operationNotFoundMarker)
}

func isSessionNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), sessionNotFoundMarker)
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
		retryPolicies:  DefaultRetryPolicyRegistry,
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
