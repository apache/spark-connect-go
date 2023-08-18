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
	"errors"
	"fmt"

	"github.com/apache/spark-connect-go/v34/client/channel"
	proto "github.com/apache/spark-connect-go/v34/internal/generated"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"io"
)

var SparkSession sparkSessionBuilderEntrypoint

type sparkSession interface {
	Read() DataFrameReader
	Sql(query string) (DataFrame, error)
	Stop() error
}

type sparkSessionBuilderEntrypoint struct {
	Builder SparkSessionBuilder
}

type SparkSessionBuilder struct {
	connectionString string
}

func (s SparkSessionBuilder) Remote(connectionString string) SparkSessionBuilder {
	copy := s
	copy.connectionString = connectionString
	return copy
}

func (s SparkSessionBuilder) Build() (sparkSession, error) {

	cb, err := channel.NewBuilder(s.connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err)
	}

	conn, err := cb.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err)
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
	return &dataFrameReaderImpl{
		sparkSession: s,
	}
}

func (s *sparkSessionImpl) Sql(query string) (DataFrame, error) {
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
	responseClient, err := s.executePlan(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sql: %s: %w", query, err)
	}
	for {
		response, err := responseClient.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to receive ExecutePlan response: %w", err)
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
	return nil, fmt.Errorf("failed to get SqlCommandResult in ExecutePlan response")
}

func (s *sparkSessionImpl) Stop() error {
	return nil
}

func (s *sparkSessionImpl) executePlan(plan *proto.Plan) (proto.SparkConnectService_ExecutePlanClient, error) {
	request := proto.ExecutePlanRequest{
		SessionId: s.sessionId,
		Plan:      plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	// Append the other items to the request.
	ctx := metadata.NewOutgoingContext(context.Background(), s.metadata)
	executePlanClient, err := s.client.ExecutePlan(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to call ExecutePlan in session %s: %w", s.sessionId, err)
	}
	return executePlanClient, nil
}

func (s *sparkSessionImpl) analyzePlan(plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
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
	ctx := metadata.NewOutgoingContext(context.Background(), s.metadata)

	response, err := s.client.AnalyzePlan(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to call AnalyzePlan in session %s: %w", s.sessionId, err)
	}
	return response, nil
}

// consumeExecutePlanClient reads through the returned GRPC stream from Spark Connect Driver. It will
// discard the returned data if there is no error. This is necessary for handling GRPC response for
// saving data frame, since such consuming will trigger Spark Connect Driver really saving data frame.
// If we do not consume the returned GRPC stream, Spark Connect Driver will not really save data frame.
func consumeExecutePlanClient(responseClient proto.SparkConnectService_ExecutePlanClient) error {
	for {
		_, err := responseClient.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			} else {
				return fmt.Errorf("failed to receive plan execution response: %w", err)
			}
		}
	}
	return nil
}
