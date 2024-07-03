package sql

import (
	"context"
	"testing"

	proto "github.com/apache/spark-connect-go/v3.5/internal/generated"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testExecutor struct {
	client   *executePlanClient
	response *proto.AnalyzePlanResponse
	err      error
}

func (t *testExecutor) executePlan(ctx context.Context, plan *proto.Plan) (*executePlanClient, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.client, nil
}

func (t *testExecutor) analyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	return t.response, nil
}

type protoClient struct {
	recvResponse  *proto.ExecutePlanResponse
	recvResponses []*proto.ExecutePlanResponse

	err error
}

func (p *protoClient) Recv() (*proto.ExecutePlanResponse, error) {
	if len(p.recvResponses) != 0 {
		p.recvResponse = p.recvResponses[0]
		p.recvResponses = p.recvResponses[1:]
	}
	return p.recvResponse, p.err
}

func (p *protoClient) Header() (metadata.MD, error) {
	return nil, p.err
}

func (p *protoClient) Trailer() metadata.MD {
	return nil
}

func (p *protoClient) CloseSend() error {
	return p.err
}

func (p *protoClient) Context() context.Context {
	return nil
}

func (p *protoClient) SendMsg(m interface{}) error {
	return p.err
}

func (p *protoClient) RecvMsg(m interface{}) error {
	return p.err
}

type connectServiceClient struct {
	t *testing.T

	analysePlanResponse        *proto.AnalyzePlanResponse
	executePlanClient          proto.SparkConnectService_ExecutePlanClient
	expectedExecutePlanRequest *proto.ExecutePlanRequest

	err error
}

func (c *connectServiceClient) ExecutePlan(ctx context.Context, in *proto.ExecutePlanRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ExecutePlanClient, error) {
	if c.expectedExecutePlanRequest != nil {
		assert.Equal(c.t, c.expectedExecutePlanRequest, in)
	}
	return c.executePlanClient, c.err
}

func (c *connectServiceClient) AnalyzePlan(ctx context.Context, in *proto.AnalyzePlanRequest, opts ...grpc.CallOption) (*proto.AnalyzePlanResponse, error) {
	return c.analysePlanResponse, c.err
}

func (c *connectServiceClient) Config(ctx context.Context, in *proto.ConfigRequest, opts ...grpc.CallOption) (*proto.ConfigResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) AddArtifacts(ctx context.Context, opts ...grpc.CallOption) (proto.SparkConnectService_AddArtifactsClient, error) {
	return nil, c.err
}

func (c *connectServiceClient) ArtifactStatus(ctx context.Context, in *proto.ArtifactStatusesRequest, opts ...grpc.CallOption) (*proto.ArtifactStatusesResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) Interrupt(ctx context.Context, in *proto.InterruptRequest, opts ...grpc.CallOption) (*proto.InterruptResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) ReattachExecute(ctx context.Context, in *proto.ReattachExecuteRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ReattachExecuteClient, error) {
	return nil, c.err
}

func (c *connectServiceClient) ReleaseExecute(ctx context.Context, in *proto.ReleaseExecuteRequest, opts ...grpc.CallOption) (*proto.ReleaseExecuteResponse, error) {
	return nil, c.err
}
