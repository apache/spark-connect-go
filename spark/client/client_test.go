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
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	proto "github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/client"
	"github.com/apache/spark-connect-go/spark/client/testutils"
	"github.com/apache/spark-connect-go/spark/mocks"
	"github.com/apache/spark-connect-go/spark/sparkerrors"
	"github.com/stretchr/testify/assert"
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

// blockingStream is a mock ExecutePlan client whose Recv blocks until release is closed,
// then returns a Canceled status — emulating a long-running server-side query.
type blockingStream struct {
	proto.SparkConnectService_ExecutePlanClient
	release chan struct{}
}

func (b *blockingStream) Recv() (*proto.ExecutePlanResponse, error) {
	<-b.release
	return nil, status.Error(codes.Canceled, "canceled")
}

func (b *blockingStream) Header() (metadata.MD, error) { return nil, nil }
func (b *blockingStream) Trailer() metadata.MD         { return nil }
func (b *blockingStream) CloseSend() error             { return nil }
func (b *blockingStream) Context() context.Context     { return context.Background() }
func (b *blockingStream) SendMsg(any) error            { return nil }
func (b *blockingStream) RecvMsg(any) error            { return nil }

// interruptRecorder wraps the testutils mock and records Interrupt invocations.
type interruptRecorder struct {
	proto.SparkConnectServiceClient
	calls   chan *proto.InterruptRequest
	release chan struct{}
}

func (i *interruptRecorder) Interrupt(ctx context.Context, in *proto.InterruptRequest,
	opts ...grpc.CallOption,
) (*proto.InterruptResponse, error) {
	i.calls <- in
	// Unblock the streaming Recv so ToTable returns.
	select {
	case <-i.release:
	default:
		close(i.release)
	}
	return &proto.InterruptResponse{SessionId: in.SessionId}, nil
}

// Regression test for issue #126: cancelling the caller's context during Collect/ExecutePlan
// must send a server-side InterruptRequest with the operation ID, not just tear down the
// gRPC stream locally.
func TestExecutePlanCancellingContextSendsInterrupt(t *testing.T) {
	release := make(chan struct{})
	stream := &blockingStream{release: release}

	underlying := testutils.NewConnectServiceClientMock(stream, nil, nil, t)
	recorder := &interruptRecorder{
		SparkConnectServiceClient: underlying,
		calls:                     make(chan *proto.InterruptRequest, 1),
		release:                   release,
	}

	c := client.NewSparkExecutorFromClient(recorder, nil, mocks.MockSessionId)

	ctx, cancel := context.WithCancel(context.Background())
	stream2, err := c.ExecutePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, _, err := stream2.ToTable()
		done <- err
	}()

	// Give the watcher goroutine a moment to be wired up, then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case req := <-recorder.calls:
		assert.Equal(t, proto.InterruptRequest_INTERRUPT_TYPE_OPERATION_ID, req.InterruptType)
		assert.NotEmpty(t, req.GetOperationId())
		assert.Equal(t, mocks.MockSessionId, req.SessionId)
	case <-time.After(2 * time.Second):
		t.Fatal("Interrupt was not invoked within 2s of ctx cancellation")
	}

	// ToTable should also unwind once Recv returns an error.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ToTable did not return after Interrupt")
	}
}

func TestInterruptAllCallsClient(t *testing.T) {
	release := make(chan struct{})
	close(release)
	recorder := &interruptRecorder{
		SparkConnectServiceClient: testutils.NewConnectServiceClientMock(nil, nil, nil, t),
		calls:                     make(chan *proto.InterruptRequest, 1),
		release:                   release,
	}
	c := client.NewSparkExecutorFromClient(recorder, nil, mocks.MockSessionId)

	resp, err := c.Interrupt(context.Background(), proto.InterruptRequest_INTERRUPT_TYPE_ALL, "")
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	req := <-recorder.calls
	assert.Equal(t, proto.InterruptRequest_INTERRUPT_TYPE_ALL, req.InterruptType)
	assert.Nil(t, req.Interrupt)
}

func TestInterruptOperationCallsClient(t *testing.T) {
	release := make(chan struct{})
	close(release)
	recorder := &interruptRecorder{
		SparkConnectServiceClient: testutils.NewConnectServiceClientMock(nil, nil, nil, t),
		calls:                     make(chan *proto.InterruptRequest, 1),
		release:                   release,
	}
	c := client.NewSparkExecutorFromClient(recorder, nil, mocks.MockSessionId)

	opID := uuid.NewString()
	_, err := c.Interrupt(context.Background(), proto.InterruptRequest_INTERRUPT_TYPE_OPERATION_ID, opID)
	assert.NoError(t, err)
	req := <-recorder.calls
	assert.Equal(t, proto.InterruptRequest_INTERRUPT_TYPE_OPERATION_ID, req.InterruptType)
	assert.Equal(t, opID, req.GetOperationId())
}
