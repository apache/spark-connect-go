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

package sql

import (
	"context"
	"fmt"

	"github.com/apache/spark-connect-go/v1/client/channel"
	"github.com/apache/spark-connect-go/v1/client/sparkerrors"
	proto "github.com/apache/spark-connect-go/v1/internal/generated"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type SparkSession interface {
	Read() DataFrameReader
	Sql(ctx context.Context, query string) (DataFrame, error)
	Stop() error
}

// NewSessionBuilder creates a new session builder for starting a new spark session
func NewSessionBuilder() *SparkSessionBuilder {
	return &SparkSessionBuilder{}
}

type SparkSessionBuilder struct {
	connectionString string
}

// Remote sets the connection string for remote connection
func (s *SparkSessionBuilder) Remote(connectionString string) *SparkSessionBuilder {
	s.connectionString = connectionString
	return s
}

func (s *SparkSessionBuilder) Build(ctx context.Context) (SparkSession, error) {

	cb, err := channel.NewBuilder(s.connectionString)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err), sparkerrors.ConnectionError)
	}

	conn, err := cb.Build(ctx)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err), sparkerrors.ConnectionError)
	}

	// Add metadata to the request.
	meta := metadata.MD{}
	for k, v := range cb.Headers {
		meta[k] = append(meta[k], v)
	}

	client := proto.NewSparkConnectServiceClient(conn)
	return &sparkSessionImpl{
		sessionId: uuid.NewString(),
		client:    client,
		metadata:  meta,
	}, nil
}

type sparkSessionImpl struct {
	sessionId string
	client    proto.SparkConnectServiceClient
	metadata  metadata.MD
}

func (s *sparkSessionImpl) Read() DataFrameReader {
	return newDataframeReader(s)
}

func (s *sparkSessionImpl) Sql(ctx context.Context, query string) (DataFrame, error) {
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_SqlCommand{
					SqlCommand: &proto.SqlCommand{
						Sql: query,
					},
				},
			},
		},
	}
	responseClient, err := s.executePlan(ctx, plan)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to execute sql: %s: %w", query, err), sparkerrors.ExecutionError)
	}
	for {
		response, err := responseClient.Recv()
		if err != nil {
			return nil, sparkerrors.WithType(fmt.Errorf("failed to receive ExecutePlan response: %w", err), sparkerrors.ReadError)
		}
		sqlCommandResult := response.GetSqlCommandResult()
		if sqlCommandResult == nil {
			continue
		}
		return &dataFrameImpl{
			sparkSession: s,
			relation:     sqlCommandResult.GetRelation(),
		}, nil
	}
}

func (s *sparkSessionImpl) Stop() error {
	return nil
}

func (s *sparkSessionImpl) executePlan(ctx context.Context, plan *proto.Plan) (*executePlanClient, error) {
	request := proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	// Append the other items to the request.
	ctx = metadata.NewOutgoingContext(ctx, s.metadata)
	client, err := s.client.ExecutePlan(ctx, &request)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err), sparkerrors.ExecutionError)
	}
	return newExecutePlanClient(client), nil
}

func (s *sparkSessionImpl) analyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
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
