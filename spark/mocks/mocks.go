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

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
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
