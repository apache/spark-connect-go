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

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/client"
	"github.com/apache/spark-connect-go/v35/spark/client/channel"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type SparkSession interface {
	Read() DataFrameReader
	Sql(ctx context.Context, query string) (DataFrame, error)
	Stop() error
	Table(name string) (DataFrame, error)
}

// NewSessionBuilder creates a new session builder for starting a new spark session
func NewSessionBuilder() *SparkSessionBuilder {
	return &SparkSessionBuilder{}
}

type SparkSessionBuilder struct {
	connectionString string
	channelBuilder   channel.Builder
}

// Remote sets the connection string for remote connection
func (s *SparkSessionBuilder) Remote(connectionString string) *SparkSessionBuilder {
	s.connectionString = connectionString
	return s
}

func (s *SparkSessionBuilder) WithChannelBuilder(cb channel.Builder) *SparkSessionBuilder {
	s.channelBuilder = cb
	return s
}

func (s *SparkSessionBuilder) Build(ctx context.Context) (SparkSession, error) {
	if s.channelBuilder == nil {
		cb, err := channel.NewBuilder(s.connectionString)
		if err != nil {
			return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err), sparkerrors.ConnectionError)
		}
		s.channelBuilder = cb
	}
	conn, err := s.channelBuilder.Build(ctx)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w", s.connectionString, err), sparkerrors.ConnectionError)
	}

	// Add metadata to the request.
	meta := metadata.MD{}
	for k, v := range s.channelBuilder.Headers() {
		meta[k] = append(meta[k], v)
	}

	sessionId := uuid.NewString()
	return &sparkSessionImpl{
		sessionId: sessionId,
		client:    client.NewSparkExecutor(conn, meta, sessionId),
	}, nil
}

type sparkSessionImpl struct {
	sessionId string
	client    client.SparkExecutor
}

func (s *sparkSessionImpl) Read() DataFrameReader {
	return NewDataframeReader(s)
}

// Sql executes a sql query and returns the result as a DataFrame
func (s *sparkSessionImpl) Sql(ctx context.Context, query string) (DataFrame, error) {
	// Due to the nature of Spark, we have to first submit the SQL query immediately as a command
	// to make sure that all side effects have been executed properly. If no side effects are present,
	// then simply prepare this as a SQL relation.

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
	// We need an execute command here.
	_, _, properties, err := s.client.ExecuteCommand(ctx, plan)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to execute sql: %s: %w", query, err), sparkerrors.ExecutionError)
	}

	val, ok := properties["sql_command_result"]
	if !ok {
		plan := &proto.Relation{
			Common: &proto.RelationCommon{
				PlanId: newPlanId(),
			},
			RelType: &proto.Relation_Sql{
				Sql: &proto.SQL{
					Query: query,
				},
			},
		}
		return NewDataFrame(s, plan), nil
	} else {
		rel := val.(*proto.Relation)
		rel.Common = &proto.RelationCommon{
			PlanId: newPlanId(),
		}
		return NewDataFrame(s, rel), nil
	}
}

func (s *sparkSessionImpl) Stop() error {
	return nil
}

func (s *sparkSessionImpl) Table(name string) (DataFrame, error) {
	return s.Read().Table(name)
}
