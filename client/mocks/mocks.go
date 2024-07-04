package mocks

import (
	"context"

	proto "github.com/apache/spark-connect-go/v3.5/internal/generated"
	"google.golang.org/grpc/metadata"
)

type ProtoClient struct {
	RecvResponse  *proto.ExecutePlanResponse
	RecvResponses []*proto.ExecutePlanResponse

	Err error
}

func (p *ProtoClient) Recv() (*proto.ExecutePlanResponse, error) {
	if len(p.RecvResponses) != 0 {
		p.RecvResponse = p.RecvResponses[0]
		p.RecvResponses = p.RecvResponses[1:]
	}
	return p.RecvResponse, p.Err
}

func (p *ProtoClient) Header() (metadata.MD, error) {
	return nil, p.Err
}

func (p *ProtoClient) Trailer() metadata.MD {
	return nil
}

func (p *ProtoClient) CloseSend() error {
	return p.Err
}

func (p *ProtoClient) Context() context.Context {
	return nil
}

func (p *ProtoClient) SendMsg(m interface{}) error {
	return p.Err
}

func (p *ProtoClient) RecvMsg(m interface{}) error {
	return p.Err
}
