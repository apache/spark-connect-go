package mocks

import (
	"context"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"google.golang.org/grpc/metadata"
)

type MockResponse struct {
	Resp *proto.ExecutePlanResponse
	Err  error
}

type ProtoClient struct {
	// The stream of responses to return.
	RecvResponse []*MockResponse
	sent         int
}

func (p *ProtoClient) Recv() (*proto.ExecutePlanResponse, error) {
	val := p.RecvResponse[p.sent]
	p.sent += 1
	return val.Resp, val.Err
}

func (p *ProtoClient) Header() (metadata.MD, error) {
	return nil, p.RecvResponse[p.sent].Err
}

func (p *ProtoClient) Trailer() metadata.MD {
	return nil
}

func (p *ProtoClient) CloseSend() error {
	return p.RecvResponse[p.sent].Err
}

func (p *ProtoClient) Context() context.Context {
	return nil
}

func (p *ProtoClient) SendMsg(m interface{}) error {
	return p.RecvResponse[p.sent].Err
}

func (p *ProtoClient) RecvMsg(m interface{}) error {
	return p.RecvResponse[p.sent].Err
}
