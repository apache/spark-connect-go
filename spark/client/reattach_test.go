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
// an execution the server has no record of.
func execRequest(operationId string) *proto.ExecutePlanRequest {
	return &proto.ExecutePlanRequest{SessionId: testSessionId, OperationId: &operationId}
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
