//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mocks

import (
	"context"
	"io"

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

// MockResponseDone is a response that indicates the plan execution is done.
var ExecutePlanResponseDone = MockResponse{
	Resp: &proto.ExecutePlanResponse{
		ResponseType: &proto.ExecutePlanResponse_ResultComplete_{
			ResultComplete: &proto.ExecutePlanResponse_ResultComplete{},
		},
	},
	Err: nil,
}

var ExecutePlanResponseEOF = MockResponse{
	Err: io.EOF,
}

// NewProtoClientMock creates a new mock client that returns the given responses.
func NewProtoClientMock(responses ...*MockResponse) *ProtoClient {
	return &ProtoClient{RecvResponse: responses}
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

func NewSqlCommand(sql string) *proto.Plan {
	return &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_SqlCommand{
					SqlCommand: &proto.SqlCommand{
						Sql: sql,
					},
				},
			},
		},
	}
}
