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

package testutils

import (
	"context"
	"testing"

	proto "github.com/apache/spark-connect-go/v40/internal/generated"
	"google.golang.org/grpc"
)

// connectServiceClient is a mock implementation of the SparkConnectServiceClient interface.
type connectServiceClient struct {
	t *testing.T

	analysePlanResponse *proto.AnalyzePlanResponse
	executePlanClient   proto.SparkConnectService_ExecutePlanClient

	err error
}

// FetchErrorDetails implements generated.SparkConnectServiceClient.
func (c *connectServiceClient) FetchErrorDetails(ctx context.Context,
	in *proto.FetchErrorDetailsRequest, opts ...grpc.CallOption,
) (*proto.FetchErrorDetailsResponse, error) {
	panic("unimplemented")
}

// ReleaseSession implements generated.SparkConnectServiceClient.
func (c *connectServiceClient) ReleaseSession(ctx context.Context, in *proto.ReleaseSessionRequest,
	opts ...grpc.CallOption,
) (*proto.ReleaseSessionResponse, error) {
	panic("unimplemented")
}

func (c *connectServiceClient) ExecutePlan(ctx context.Context, in *proto.ExecutePlanRequest,
	opts ...grpc.CallOption,
) (proto.SparkConnectService_ExecutePlanClient, error) {
	return c.executePlanClient, c.err
}

func (c *connectServiceClient) AnalyzePlan(ctx context.Context, in *proto.AnalyzePlanRequest,
	opts ...grpc.CallOption,
) (*proto.AnalyzePlanResponse, error) {
	return c.analysePlanResponse, c.err
}

func (c *connectServiceClient) Config(ctx context.Context, in *proto.ConfigRequest, opts ...grpc.CallOption) (*proto.ConfigResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) AddArtifacts(ctx context.Context, opts ...grpc.CallOption) (proto.SparkConnectService_AddArtifactsClient, error) {
	return nil, c.err
}

func (c *connectServiceClient) ArtifactStatus(ctx context.Context,
	in *proto.ArtifactStatusesRequest, opts ...grpc.CallOption,
) (*proto.ArtifactStatusesResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) Interrupt(ctx context.Context, in *proto.InterruptRequest,
	opts ...grpc.CallOption,
) (*proto.InterruptResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) ReattachExecute(ctx context.Context,
	in *proto.ReattachExecuteRequest, opts ...grpc.CallOption,
) (proto.SparkConnectService_ReattachExecuteClient, error) {
	return c.executePlanClient, c.err
}

func (c *connectServiceClient) ReleaseExecute(ctx context.Context, in *proto.ReleaseExecuteRequest,
	opts ...grpc.CallOption,
) (*proto.ReleaseExecuteResponse, error) {
	return nil, c.err
}

func NewConnectServiceClientMock(epc proto.SparkConnectService_ExecutePlanClient,
	apr *proto.AnalyzePlanResponse, err error, t *testing.T,
) proto.SparkConnectServiceClient {
	return &connectServiceClient{
		t:                   t,
		analysePlanResponse: apr,
		executePlanClient:   epc,
		err:                 err,
	}
}
