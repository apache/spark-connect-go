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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/uuid"

	proto "github.com/apache/spark-connect-go/internal/generated"
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

var (
	MockSessionId   = uuid.NewString()
	MockOperationId = uuid.NewString()
	MockResponseId  = "1"
	MockUserContext = &proto.UserContext{
		UserId: "user",
	}
)

// MockResponseDone is a response that indicates the plan execution is done.
var ExecutePlanResponseDone = MockResponse{
	Resp: &proto.ExecutePlanResponse{
		ResponseType: &proto.ExecutePlanResponse_ResultComplete_{
			ResultComplete: &proto.ExecutePlanResponse_ResultComplete{},
		},
		SessionId:   MockSessionId,
		OperationId: MockOperationId,
	},
	Err: nil,
}

var ExecutePlanResponseEOF = MockResponse{
	Err: io.EOF,
}

var ExecutePlanResponseBrokenSchema = MockResponse{
	Resp: &proto.ExecutePlanResponse{
		Schema: &proto.DataType{
			Kind: &proto.DataType_String_{
				String_: &proto.DataType_String{},
			},
		},
		SessionId:   MockSessionId,
		OperationId: MockOperationId,
	},
}

var ExecutePlanResponseWithSchema = MockResponse{
	Resp: &proto.ExecutePlanResponse{
		ResponseId:  MockResponseId,
		OperationId: MockOperationId,
		Schema: &proto.DataType{
			Kind: &proto.DataType_Struct_{
				Struct: &proto.DataType_Struct{
					Fields: []*proto.DataType_StructField{
						{
							Name: "col0",
							DataType: &proto.DataType{
								Kind: &proto.DataType_Integer_{
									Integer: &proto.DataType_Integer{},
								},
							},
							Nullable: true,
						},
					},
				},
			},
		},
	},
}

var ExecutePlanResponseUnavailable = MockResponse{
	Err: status.New(codes.Unavailable, "Unavailable").Err(),
}

var ExecutePlanRequestSql = proto.ExecutePlanRequest{
	Plan:        NewSqlCommand("select range(10)"),
	OperationId: &MockOperationId,
	SessionId:   MockSessionId,
	UserContext: MockUserContext,
}

var AnalyzePlanRequestSql = proto.AnalyzePlanRequest{
	SessionId: MockSessionId,
	Analyze: &proto.AnalyzePlanRequest_Schema_{
		Schema: &proto.AnalyzePlanRequest_Schema{
			Plan: NewSqlCommand("select range(10)"),
		},
	},
	UserContext: MockUserContext,
}

var AnalyzePlanResponse = &proto.AnalyzePlanResponse{
	SessionId: MockSessionId,
	Result: &proto.AnalyzePlanResponse_Schema_{
		Schema: &proto.AnalyzePlanResponse_Schema{
			Schema: &proto.DataType{
				Kind: &proto.DataType_Struct_{
					Struct: &proto.DataType_Struct{
						Fields: []*proto.DataType_StructField{
							{
								Name: "col0",
								DataType: &proto.DataType{
									Kind: &proto.DataType_Integer_{
										Integer: &proto.DataType_Integer{},
									},
								},
							},
						},
					},
				},
			},
		},
	},
}

// NewProtoClientMock creates a new mock client that returns the given responses.
func NewProtoClientMock(responses ...*MockResponse) proto.SparkConnectService_ExecutePlanClient {
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
