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
	"io"
	"testing"

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

	stream := NewExecuteResponseStream(first, testSessionId, "op-1", reattachOpts(), rpc, nil)
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

	stream := NewExecuteResponseStream(first, testSessionId, "op-2", reattachOpts(), rpc, nil)
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

	stream := NewExecuteResponseStream(first, testSessionId, "op-3", reattachOpts(), rpc, nil)
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

	stream := NewExecuteResponseStream(first, testSessionId, "op-4", reattachOpts(), rpc, nil)
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

	stream := NewExecuteResponseStream(first, testSessionId, "op-4", options.DefaultSparkClientOptions, rpc, nil)
	_, _, err := stream.ToTable(context.Background())

	require.NoError(t, err)
	assert.Empty(t, rpc.requests, "reattach must not be issued when the option is off")
}
