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
	"errors"
	"fmt"
	"io"

	"github.com/apache/spark-connect-go/v35/internal/generated"
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SparkExecutor is the interface for executing a plan in Spark.
//
// This interface does not deal with the public Spark API abstractions but roughly deals on the
// RPC API level and the necessary translation of Arrow to Row objects.
type SparkExecutor interface {
	ExecutePlan(ctx context.Context, plan *generated.Plan) (*ExecutePlanClient, error)
	AnalyzePlan(ctx context.Context, plan *generated.Plan) (*generated.AnalyzePlanResponse, error)
}

type SparkExecutorImpl struct {
	client    proto.SparkConnectServiceClient
	metadata  metadata.MD
	sessionId string
}

func (s *SparkExecutorImpl) ExecutePlan(ctx context.Context, plan *proto.Plan) (*ExecutePlanClient, error) {
	request := proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}

	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)
	c, err := s.client.ExecutePlan(ctx, &request)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	return NewExecutePlanClient(c), nil
}

func (s *SparkExecutorImpl) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	request := proto.AnalyzePlanRequest{
		SessionId: s.sessionId,
		Analyze: &proto.AnalyzePlanRequest_Schema_{
			Schema: &proto.AnalyzePlanRequest_Schema{
				Plan: plan,
			},
		},
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to call AnalyzePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	return response, nil
}

func NewSparkExecutor(conn *grpc.ClientConn, md metadata.MD, sessionId string) SparkExecutor {
	client := proto.NewSparkConnectServiceClient(conn)
	return &SparkExecutorImpl{
		client:    client,
		metadata:  md,
		sessionId: sessionId,
	}
}

func NewSparkExecutorFromClient(client proto.SparkConnectServiceClient, md metadata.MD, sessionId string) SparkExecutor {
	return &SparkExecutorImpl{
		client:    client,
		metadata:  md,
		sessionId: sessionId,
	}
}

type ExecutePlanClient struct {
	generated.SparkConnectService_ExecutePlanClient
}

func NewExecutePlanClient(responseClient proto.SparkConnectService_ExecutePlanClient) *ExecutePlanClient {
	return &ExecutePlanClient{
		responseClient,
	}
}

// consumeAll reads through the returned GRPC stream from Spark Connect Driver. It will
// discard the returned data if there is no error. This is necessary for handling GRPC response for
// saving data frame, since such consuming will trigger Spark Connect Driver really saving data frame.
// If we do not consume the returned GRPC stream, Spark Connect Driver will not really save data frame.
func (c *ExecutePlanClient) ConsumeAll() error {
	for {
		_, err := c.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			} else {
				return sparkerrors.WithType(fmt.Errorf("failed to receive plan execution response: %w", err), sparkerrors.ReadError)
			}
		}
	}
}
