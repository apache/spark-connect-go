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
	"sync"
	"testing"
	"time"

	proto "github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/client/options"
	"github.com/apache/spark-connect-go/spark/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testSessionId = "test-session"

// reattachRPCMock implements only ReattachExecute. The embedded interface is
// nil, so any other call panics rather than silently returning a zero value.
type reattachRPCMock struct {
	proto.SparkConnectServiceClient
	streams  []proto.SparkConnectService_ExecutePlanClient
	requests []*proto.ReattachExecuteRequest
	err      error

	// Releases are sent from a background goroutine, so they need a lock and
	// have to be asserted with Eventually rather than read directly.
	mu       sync.Mutex
	releases []*proto.ReleaseExecuteRequest

	// Set when the execution is started over because the server did not
	// recognise it.
	restartStream proto.SparkConnectService_ExecutePlanClient
	executePlans  []*proto.ExecutePlanRequest
}

// errUnknownHandle is how the server reports an execution it has no record of.
var errUnknownHandle = errors.New(
	"INVALID_HANDLE.OPERATION_NOT_FOUND: operation not found")

func (m *reattachRPCMock) ReleaseExecute(
	_ context.Context, in *proto.ReleaseExecuteRequest, _ ...grpc.CallOption,
) (*proto.ReleaseExecuteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releases = append(m.releases, in)
	return &proto.ReleaseExecuteResponse{}, nil
}

// releasedAll reports whether a release_all has arrived yet.
func (m *reattachRPCMock) releasedAll() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.releases {
		if r.GetReleaseAll() != nil {
			return true
		}
	}
	return false
}

// releasedUntil returns the response ids the client asked the server to drop.
func (m *reattachRPCMock) releasedUntil() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, r := range m.releases {
		if u := r.GetReleaseUntil(); u != nil {
			out = append(out, u.GetResponseId())
		}
	}
	return out
}

func (m *reattachRPCMock) ExecutePlan(
	_ context.Context, in *proto.ExecutePlanRequest, _ ...grpc.CallOption,
) (proto.SparkConnectService_ExecutePlanClient, error) {
	m.executePlans = append(m.executePlans, in)
	return m.restartStream, nil
}

func (m *reattachRPCMock) ReattachExecute(
	_ context.Context, in *proto.ReattachExecuteRequest, _ ...grpc.CallOption,
) (proto.SparkConnectService_ReattachExecuteClient, error) {
	m.requests = append(m.requests, in)
	if m.err != nil {
		return nil, m.err
	}
	// Hands back a fresh stream per reattach, reusing the last one once they are
	// exhausted so a stalled-server test can keep reattaching.
	idx := len(m.requests) - 1
	if idx >= len(m.streams) {
		idx = len(m.streams) - 1
	}
	return m.streams[idx], nil
}

// execRequest is the originating ExecutePlan the stream keeps so it can restart
// an execution the server has no record of. Rooted in a relation the client
// composed itself, which is the shape that can be replayed without repeating
// anything -- deliberately not a SQL-text relation, which cannot (see
// TestToTable_RefusesToRestartRelationsItCannotVouchFor).
func execRequest(operationId string) *proto.ExecutePlanRequest {
	return &proto.ExecutePlanRequest{
		SessionId:   testSessionId,
		OperationId: &operationId,
		Plan: &proto.Plan{
			OpType: &proto.Plan_Root{
				Root: &proto.Relation{
					RelType: &proto.Relation_Range{Range: &proto.Range{End: 10, Step: 1}},
				},
			},
		},
	}
}

// commandRequest is the originating ExecutePlan for a write, i.e. a plan whose
// effects the server may already have applied.
func commandRequest(operationId string) *proto.ExecutePlanRequest {
	return &proto.ExecutePlanRequest{
		SessionId:   testSessionId,
		OperationId: &operationId,
		Plan: &proto.Plan{
			OpType: &proto.Plan_Command{
				Command: &proto.Command{
					CommandType: &proto.Command_WriteOperation{
						WriteOperation: &proto.WriteOperation{
							Mode:     proto.WriteOperation_SAVE_MODE_APPEND,
							SaveType: &proto.WriteOperation_Path{Path: "/tmp/out"},
						},
					},
				},
			},
		},
	}
}

func response(responseId string) *mocks.MockResponse {
	return &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{SessionId: testSessionId, ResponseId: responseId},
	}
}

func resultComplete(responseId string) *mocks.MockResponse {
	return &mocks.MockResponse{
		Resp: &proto.ExecutePlanResponse{
			SessionId:    testSessionId,
			ResponseId:   responseId,
			ResponseType: &proto.ExecutePlanResponse_ResultComplete_{},
		},
	}
}

func eof() *mocks.MockResponse {
	return &mocks.MockResponse{Err: io.EOF}
}

func reattachOpts() options.SparkClientOptions {
	return options.NewSparkClientOptions(true)
}

// fastResumeStream is the stream under test carrying the testing retry policy
// instead of the default one, so a test that spends the whole resume budget does
// not have to sit through the real policy's minutes of exponential backoff.
func fastResumeStream(
	first proto.SparkConnectService_ExecutePlanClient,
	request *proto.ExecutePlanRequest,
	rpc *reattachRPCMock,
) *ExecutePlanClient {
	stream := NewExecuteResponseStream(first, testSessionId, request, reattachOpts(), rpc, nil)
	client := stream.(*ExecutePlanClient)
	client.retryPolicies = []RetryPolicy{TestingRetryPolicy}
	return client
}

// unavailable is the stream failure the retry policies call transient.
func unavailable() *mocks.MockResponse {
	return &mocks.MockResponse{Err: status.Error(codes.Unavailable, "connection reset")}
}

// TestNewExecutePlanRequestAsksTheServerToMakeItReattachable is the other end of
// the option's path: none of the resume machinery is reachable unless the
// request itself tells the server the execution may be reattached to, and the
// server only buffers responses and rotates streams when it does.
func TestNewExecutePlanRequestAsksTheServerToMakeItReattachable(t *testing.T) {
	for _, reattach := range []bool{true, false} {
		impl := &sparkConnectClientImpl{
			sessionId: testSessionId,
			opts:      options.NewSparkClientOptions(reattach),
		}
		request := impl.newExecutePlanRequest(&proto.Plan{})

		require.Len(t, request.GetRequestOptions(), 1)
		reattachOptions := request.GetRequestOptions()[0].GetReattachOptions()
		require.NotNil(t, reattachOptions)
		assert.Equal(t, reattach, reattachOptions.GetReattachable(),
			"the request must carry the configured setting, not a fixed one")
		assert.NotEmpty(t, request.GetOperationId(),
			"reattaching needs a client-assigned operation id to quote")
	}
}

// TestToTable_ResumesWhenStreamEndsBeforeResultComplete is the behaviour the
// whole reattach mechanism exists for. Under reattachable execution the server
// deliberately ends the response stream on its own schedule -- after
// spark.connect.execute.reattachable.senderMaxStreamDuration, two minutes by
// default -- while the query is still running. An EOF is therefore not the end
// of the result; only ResultComplete is. Treating that EOF as the end is what
// made long queries fail with "the result is not complete".
func TestToTable_ResumesWhenStreamEndsBeforeResultComplete(t *testing.T) {
	// The server hands over one response, then ends the stream without ever
	// saying the result was complete.
	first := mocks.NewProtoClientMock(response("r1"), eof())
	resumed := mocks.NewProtoClientMock(resultComplete("r2"), eof())
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{resumed}}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-1"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())
	require.NoError(t, err, "an early stream end must be resumed, not reported as a truncated result")

	require.Len(t, rpc.requests, 1, "exactly one reattach was needed")
	assert.Equal(t, "op-1", rpc.requests[0].GetOperationId())
	// Resuming from the last response already seen is what stops the server
	// replaying batches the client has already accumulated.
	assert.Equal(t, "r1", rpc.requests[0].GetLastResponseId(),
		"reattach must resume after the last response received, not from the start")
}

// TestToTable_ReattachOmitsLastResponseIdWhenNothingSeen covers the stream that
// ends before delivering anything: there is no response to resume after, and
// sending an empty last_response_id would ask the server to continue after a
// response it never sent.
func TestToTable_ReattachOmitsLastResponseIdWhenNothingSeen(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	resumed := mocks.NewProtoClientMock(resultComplete("r1"), eof())
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{resumed}}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-2"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())
	require.NoError(t, err)

	require.Len(t, rpc.requests, 1)
	assert.Nil(t, rpc.requests[0].LastResponseId, "no response seen yet means no resume point to quote")
}

// TestToTable_ToleratesRotationsThatDeliverNothing is the case that a
// progress-counting guard gets wrong, and it is the common one, not an edge
// case. A command such as a parquet write emits no responses at all until it
// finishes, so a query running for minutes rotates through several streams that
// each deliver nothing. Treating an empty rotation as a stall would cap how
// long a query is allowed to take -- observed against production EMR
// Serverless, where a ~8 minute export rotated four times before producing
// anything.
func TestToTable_ToleratesRotationsThatDeliverNothing(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	empty := func() proto.SparkConnectService_ExecutePlanClient {
		return mocks.NewProtoClientMock(eof())
	}
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{
		empty(), empty(), empty(), empty(),
		mocks.NewProtoClientMock(resultComplete("r1"), eof()),
	}}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-3"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err, "silent rotations are a running query, not a wedged one")
	assert.Len(t, rpc.requests, 5, "each empty rotation must be resumed rather than abandoned")
}

// TestToTable_StopsWhenContextCancelled is what bounds the loop now that empty
// rotations are not counted: the caller's context, not a retry budget. Without
// this a server that never completes would be reattached to forever.
func TestToTable_StopsWhenContextCancelled(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	// Always empty, so only cancellation can end the loop.
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{
		mocks.NewProtoClientMock(eof(), eof(), eof(), eof(), eof(), eof(), eof(), eof()),
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-4"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(ctx)

	require.Error(t, err, "a cancelled context must stop the reattach loop")
	assert.Contains(t, err.Error(), "gave up reattaching")
}

// TestToTable_DoesNotReattachWhenDisabled pins the default path. With
// reattachable execution off the server keeps one stream open for the whole
// query, so an EOF really is the end of the result and reattaching would be
// wrong.
func TestToTable_DoesNotReattachWhenDisabled(t *testing.T) {
	first := mocks.NewProtoClientMock(response("r1"), eof())
	rpc := &reattachRPCMock{}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-4"), options.DefaultSparkClientOptions, rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err)
	assert.Empty(t, rpc.requests, "reattach must not be issued when the option is off")
}

// TestToTable_ReleasesBufferedResponses covers the other half of reattachable
// execution: the server buffers responses so a resumed stream can backtrack, and
// only stops once the client says it is safe. Without this the buffer is held
// until the execution ages out, which is what the reference clients avoid with
// release_until as they consume and release_all at the end.
func TestToTable_ReleasesBufferedResponses(t *testing.T) {
	first := mocks.NewProtoClientMock(response("r1"), resultComplete("r2"), eof())
	rpc := &reattachRPCMock{}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-5"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())
	require.NoError(t, err)

	require.Eventually(t, rpc.releasedAll, time.Second, 10*time.Millisecond,
		"a completed result must disown the execution so the server can drop it")
	assert.Contains(t, rpc.releasedUntil(), "r1",
		"each consumed response must be released so the server stops buffering it")
}

// TestToTable_DoesNotReleaseWhenReattachDisabled pins that the extra RPCs only
// exist for reattachable executions -- a non-reattachable one buffers nothing,
// so releasing it would be pure overhead on every query.
func TestToTable_DoesNotReleaseWhenReattachDisabled(t *testing.T) {
	first := mocks.NewProtoClientMock(response("r1"), eof())
	rpc := &reattachRPCMock{}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-6"), options.DefaultSparkClientOptions, rpc, nil)
	_, _, err := stream.ToTable(context.Background())
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond) // give any stray background release a chance to land
	assert.Empty(t, rpc.releasedUntil())
	assert.False(t, rpc.releasedAll())
}

// TestToTable_RestartsWhenServerHasNoRecordOfTheOperation covers the case the
// client sets its own operation id for: if the original ExecutePlan never
// reached the server, reattaching fails with INVALID_HANDLE.OPERATION_NOT_FOUND.
// Nothing ran, so starting over is safe and is what the reference clients do.
func TestToTable_RestartsWhenServerHasNoRecordOfTheOperation(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	rpc := &reattachRPCMock{
		err:           errUnknownHandle,
		restartStream: mocks.NewProtoClientMock(resultComplete("r1"), eof()),
	}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-7"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err, "an execution the server never received must be started over")
	require.Len(t, rpc.executePlans, 1, "exactly one restart")
	assert.Equal(t, "op-7", rpc.executePlans[0].GetOperationId(),
		"the restart must reuse the operation id so it stays reattachable")
}

// TestToTable_RefusesToRestartAfterResponsesReceived is the safety rule on that
// path. Once responses have been consumed, a fresh execution would replay them
// and silently duplicate rows, so this has to fail loudly instead.
func TestToTable_RefusesToRestartAfterResponsesReceived(t *testing.T) {
	first := mocks.NewProtoClientMock(response("r1"), eof())
	rpc := &reattachRPCMock{
		err:           errUnknownHandle,
		restartStream: mocks.NewProtoClientMock(resultComplete("r2"), eof()),
	}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-8"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err, "restarting after partial consumption would duplicate rows")
	assert.Contains(t, err.Error(), "would duplicate")
	assert.Empty(t, rpc.executePlans, "no restart may be attempted once responses are in hand")
}

// TestToTable_RefusesToRestartACommand is the other half of that safety rule,
// and the one an empty stream hides. A command emits no responses at all until
// it finishes, so lastResponseId stays empty for its whole run -- which means an
// append the server executed and then forgot is indistinguishable from one it
// never received. Restarting on that guess would write the data twice.
func TestToTable_RefusesToRestartACommand(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	rpc := &reattachRPCMock{
		err:           errUnknownHandle,
		restartStream: mocks.NewProtoClientMock(resultComplete("r1"), eof()),
	}

	stream := NewExecuteResponseStream(first, testSessionId, commandRequest("op-9"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err, "a command must never be replayed on the strength of an empty stream")
	assert.Contains(t, err.Error(), "is not known to be free of side effects")
	assert.Contains(t, err.Error(), "Command_WriteOperation",
		"the error must name what it refused, so a missing case is not mistaken for a deliberate one")
	assert.Empty(t, rpc.executePlans, "the write must not be issued a second time")
}

// TestToTable_RefusesToRestartRelationsItCannotVouchFor is the set of cases that
// being rooted in a relation does not cover, and the reason the guard lists what
// is safe instead of what is dangerous. Each of these changes server state, or
// runs something this client cannot characterise, through a plan GetRoot()
// reports as a relation -- and each reaches restart with an empty
// lastResponseId, because none of them has to emit a response before taking
// effect. So the earlier guards cannot catch any of them.
//
// The SQL case is the one reachable through this library today: Sql submits a
// query as a command to get the side effects over with, then wraps the same text
// as a Relation_Sql for the DataFrame it returns, and the server applies an
// INSERT or a DDL statement again while planning that.
func TestToTable_RefusesToRestartRelationsItCannotVouchFor(t *testing.T) {
	cases := []struct {
		name string
		root *proto.Relation
		// shape is what the error must name, so a refusal stays diagnosable: it is
		// the difference between "refused on purpose" and "the list is missing a
		// case".
		shape string
	}{
		{"raw SQL text", &proto.Relation{
			RelType: &proto.Relation_Sql{Sql: &proto.SQL{Query: "insert into t select * from s"}},
		}, "Relation_Sql"},
		{"a catalog mutation", &proto.Relation{
			RelType: &proto.Relation_Catalog{Catalog: &proto.Catalog{
				CatType: &proto.Catalog_CreateTable{CreateTable: &proto.CreateTable{TableName: "t"}},
			}},
		}, "Relation_Catalog"},
		{"user code", &proto.Relation{
			RelType: &proto.Relation_MapPartitions{MapPartitions: &proto.MapPartitions{}},
		}, "Relation_MapPartitions"},
		{"an extension this client cannot read", &proto.Relation{
			RelType: &proto.Relation_Extension{},
		}, "Relation_Extension"},
		{"a relation with no type set at all", &proto.Relation{}, "a plan with nothing set"},
		{"no relation at all", nil, "a plan with nothing set"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			operationId := "op-" + tc.name
			request := &proto.ExecutePlanRequest{
				SessionId:   testSessionId,
				OperationId: &operationId,
				Plan:        &proto.Plan{OpType: &proto.Plan_Root{Root: tc.root}},
			}
			rpc := &reattachRPCMock{
				err:           errUnknownHandle,
				restartStream: mocks.NewProtoClientMock(resultComplete("r1"), eof()),
			}

			stream := NewExecuteResponseStream(
				mocks.NewProtoClientMock(eof()), testSessionId, request, reattachOpts(), rpc, nil)
			_, _, err := stream.ToTable(context.Background())

			require.Error(t, err, "a plan that may have taken effect already must not be replayed")
			assert.Contains(t, err.Error(), "is not known to be free of side effects")
			assert.Contains(t, err.Error(), tc.shape, "the error must name the shape it refused")
			assert.Empty(t, rpc.executePlans, "it must not be issued a second time")
		})
	}
}

// TestToTable_RefusesToRestartAnUnclassifiablePlan keeps the guard above failing
// closed. It asks whether the plan is a query rather than whether it is a
// command, so a plan it cannot place -- an absent one, or an op type added to
// the proto later -- is refused instead of being replayed by default.
func TestToTable_RefusesToRestartAnUnclassifiablePlan(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	rpc := &reattachRPCMock{
		err:           errUnknownHandle,
		restartStream: mocks.NewProtoClientMock(resultComplete("r1"), eof()),
	}

	operationId := "op-15"
	planless := &proto.ExecutePlanRequest{SessionId: testSessionId, OperationId: &operationId}

	stream := NewExecuteResponseStream(first, testSessionId, planless, reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err, "a plan that cannot be shown to be read-only must not be replayed")
	assert.Contains(t, err.Error(), "is not known to be free of side effects",
		"it must be the side-effect guard that refuses this, not an unrelated one")
	assert.Empty(t, rpc.executePlans)
}

// TestToTable_RestartsAQueryAtMostOnce bounds the restart path. An execution the
// server loses a second time is not a request that failed to arrive, and looping
// on that guess would re-run the plan indefinitely.
func TestToTable_RestartsAQueryAtMostOnce(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	rpc := &reattachRPCMock{
		err: errUnknownHandle,
		// The restarted execution also ends without completing, so the client
		// comes back around to reattach and is told again that it is unknown.
		restartStream: mocks.NewProtoClientMock(eof()),
	}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-10"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err, "a second lost execution must fail rather than restart again")
	assert.Contains(t, err.Error(), "restarted once already")
	assert.Len(t, rpc.executePlans, 1, "exactly one restart, not one per rotation")
}

// TestToTable_DoesNotRestartWhenTheSessionIsGone pins the one place this client
// is deliberately stricter than the reference ones. Scala's
// ExecutePlanResponseReattachableIterator and PySpark's reattach.py match
// SESSION_NOT_FOUND in the same branch as OPERATION_NOT_FOUND and re-issue the
// original ExecutePlan.
//
// They can, because both also track the server's own session id across every
// response of a session, so a replay that lands in a session the server rebuilt
// fails on its first response. This client has no such check yet, and
// initialRequest names the original session, so restarting would run the plan in
// a fresh empty session -- without the temp views, cached tables or SQL confs the
// caller set up -- and return that result as if nothing had happened. Refusing is
// the honest answer until the check exists; then this test should be inverted.
func TestToTable_DoesNotRestartWhenTheSessionIsGone(t *testing.T) {
	first := mocks.NewProtoClientMock(eof())
	rpc := &reattachRPCMock{
		err:           errors.New("INVALID_HANDLE.SESSION_NOT_FOUND: session not found"),
		restartStream: mocks.NewProtoClientMock(resultComplete("r1"), eof()),
	}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-11"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no longer exists on the server")
	assert.Empty(t, rpc.executePlans, "a plan must not be replayed into a different session")
}

// TestToTable_ResumesWhenTheStreamBreaksMidFlight covers the rotations that are
// not clean. Only the very first stream comes from the retrying RPC client; a
// resumed stream is a plain one, so without handling a transient error here a
// long query would be *less* able to survive a blip after its first rotation
// than before it -- the opposite of what reattaching is for.
func TestToTable_ResumesWhenTheStreamBreaksMidFlight(t *testing.T) {
	first := mocks.NewProtoClientMock(
		response("r1"),
		&mocks.MockResponse{Err: status.Error(codes.Unavailable, "connection reset")},
	)
	resumed := mocks.NewProtoClientMock(resultComplete("r2"), eof())
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{resumed}}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-12"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err, "a transient stream error must be resumed, not surfaced")
	require.Len(t, rpc.requests, 1)
	assert.Equal(t, "r1", rpc.requests[0].GetLastResponseId(),
		"resuming after a break must still pick up from the last response seen")
}

// TestToTable_StopsResumingAfterRepeatedStreamFailures bounds the branch above.
// A clean rotation is paced by the server, which holds each stream open for
// senderMaxStreamDuration, so resuming immediately is fine there. A stream that
// fails on its first read costs nothing, so a server failing every resumed
// stream would otherwise be reattached to in a tight loop for as long as the
// caller's context allowed -- forever, for the many callers that pass
// context.Background().
func TestToTable_StopsResumingAfterRepeatedStreamFailures(t *testing.T) {
	first := mocks.NewProtoClientMock(unavailable())
	// Every resumed stream fails on its first read too, and the mock keeps
	// handing back its last stream, so only the budget can end this.
	rpc := &reattachRPCMock{streams: []proto.SparkConnectService_ExecutePlanClient{
		mocks.NewProtoClientMock(unavailable(), unavailable(), unavailable(), unavailable(), unavailable()),
	}}

	stream := fastResumeStream(first, execRequest("op-17"), rpc)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err, "a stream that never recovers must give up, not spin")
	assert.Contains(t, err.Error(), "gave up resuming operation")
	assert.Len(t, rpc.requests, int(TestingRetryPolicy.MaxRetries),
		"the resume budget must come from the retry policy, not be unbounded")
}

// TestToTable_ResumeBudgetResetsOnProgress keeps that bound from capping how long
// a healthy query may run. The budget is there to catch a stream that never
// recovers, so it has to count consecutive failures: a query long enough to hit
// the occasional blip would otherwise exhaust a lifetime budget and fail even
// though every blip was recovered from.
func TestToTable_ResumeBudgetResetsOnProgress(t *testing.T) {
	first := mocks.NewProtoClientMock(unavailable())
	// More blips than the budget allows, but each one separated by a response, so
	// no two are consecutive.
	blips := int(TestingRetryPolicy.MaxRetries) + 1
	streams := make([]proto.SparkConnectService_ExecutePlanClient, 0, blips)
	for i := 1; i < blips; i++ {
		streams = append(streams, mocks.NewProtoClientMock(response(fmt.Sprintf("r%d", i)), unavailable()))
	}
	streams = append(streams, mocks.NewProtoClientMock(resultComplete("done"), eof()))
	rpc := &reattachRPCMock{streams: streams}

	stream := fastResumeStream(first, execRequest("op-18"), rpc)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err, "a blip that was recovered from must not count against a later one")
	assert.Len(t, rpc.requests, blips, "every blip must still have been resumed")
}

// TestToTable_SurfacesErrorsThatResumingCannotFix keeps the branch above narrow:
// only errors the retry policies call transient are worth another stream. A
// genuine query failure has to reach the caller.
func TestToTable_SurfacesErrorsThatResumingCannotFix(t *testing.T) {
	first := mocks.NewProtoClientMock(
		response("r1"),
		&mocks.MockResponse{Err: status.Error(codes.InvalidArgument, "cannot resolve column 'nope'")},
	)
	rpc := &reattachRPCMock{}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-13"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot resolve column")
	assert.Empty(t, rpc.requests, "a query error must not be retried as if the stream had rotated")
}

// TestToTable_StopsReadingOnceTheResultIsComplete pins that nothing is read
// after ResultComplete. The execution has just been disowned with release_all,
// so an error on a further read would fail a query whose result is complete and,
// with the server told to drop it, no longer recoverable.
func TestToTable_StopsReadingOnceTheResultIsComplete(t *testing.T) {
	// The read that must never happen returns an error rather than the EOF a
	// healthy server would send.
	first := mocks.NewProtoClientMock(
		resultComplete("r1"),
		&mocks.MockResponse{Err: status.Error(codes.Unavailable, "connection reset after completion")},
	)
	rpc := &reattachRPCMock{}

	stream := NewExecuteResponseStream(first, testSessionId, execRequest("op-14"), reattachOpts(), rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err, "a complete result must not be lost to what happens on the stream afterwards")
	assert.Empty(t, rpc.requests, "a completed execution must not be reattached to")
}
