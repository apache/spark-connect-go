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

	"github.com/google/uuid"
	"google.golang.org/grpc"

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

// executePlanRecorder wraps the testutils mock and captures the last ExecutePlanRequest
// passed to ExecutePlan so tests can inspect Tags / OperationId / SessionId.
type executePlanRecorder struct {
	proto.SparkConnectServiceClient
	lastRequest *proto.ExecutePlanRequest
}

func (r *executePlanRecorder) ExecutePlan(ctx context.Context, in *proto.ExecutePlanRequest,
	opts ...grpc.CallOption,
) (proto.SparkConnectService_ExecutePlanClient, error) {
	r.lastRequest = in
	return r.SparkConnectServiceClient.ExecutePlan(ctx, in, opts...)
}

func TestAddTagRejectsInvalidInput(t *testing.T) {
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, nil, nil, t), nil, mocks.MockSessionId)

	assert.ErrorIs(t, c.AddTag(""), sparkerrors.InvalidArgumentError)
	assert.ErrorIs(t, c.AddTag("has,comma"), sparkerrors.InvalidArgumentError)
	assert.Empty(t, c.GetTags(), "invalid tags must not be stored")
}

func TestTagsRoundTripAddRemoveClear(t *testing.T) {
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, nil, nil, t), nil, mocks.MockSessionId)

	assert.NoError(t, c.AddTag("beta"))
	assert.NoError(t, c.AddTag("alpha"))
	assert.NoError(t, c.AddTag("alpha")) // dedupes
	assert.Equal(t, []string{"alpha", "beta"}, c.GetTags())

	assert.NoError(t, c.RemoveTag("alpha"))
	assert.Equal(t, []string{"beta"}, c.GetTags())

	// Removing a tag that was never added is a no-op.
	assert.NoError(t, c.RemoveTag("never-added"))
	assert.Equal(t, []string{"beta"}, c.GetTags())

	c.ClearTags()
	assert.Empty(t, c.GetTags())
}

func TestExecutePlanRequestCarriesSessionTags(t *testing.T) {
	ctx := context.Background()
	responseStream := mocks.NewProtoClientMock(&mocks.ExecutePlanResponseDone, &mocks.ExecutePlanResponseEOF)
	recorder := &executePlanRecorder{
		SparkConnectServiceClient: testutils.NewConnectServiceClientMock(responseStream, nil, nil, t),
	}
	c := client.NewSparkExecutorFromClient(recorder, nil, mocks.MockSessionId)

	// First call: no tags configured — request must not carry any.
	_, err := c.ExecutePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)
	assert.Empty(t, recorder.lastRequest.GetTags(), "untagged session must not send a Tags field")

	assert.NoError(t, c.AddTag("etl-job-42"))
	assert.NoError(t, c.AddTag("priority-high"))

	_, err = c.ExecutePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"etl-job-42", "priority-high"}, recorder.lastRequest.GetTags())

	c.ClearTags()
	_, err = c.ExecutePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)
	assert.Empty(t, recorder.lastRequest.GetTags(), "ClearTags must scrub tags from subsequent requests")
}
