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

	"github.com/apache/arrow/go/v17/arrow"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	client2 "github.com/apache/spark-connect-go/v35/spark/client"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

type testExecutor struct {
	client   *client2.ExecutePlanClient
	response *proto.AnalyzePlanResponse
	err      error
}

func (t *testExecutor) ExecutePlan(ctx context.Context, plan *proto.Plan) (*client2.ExecutePlanClient, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.client, nil
}

func (t *testExecutor) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	return t.response, nil
}

func (t *testExecutor) ExecuteCommand(ctx context.Context, plan *proto.Plan) (arrow.Table, *types.StructType, map[string]interface{}, error) {
	if t.err != nil {
		return nil, nil, nil, t.err
	}
	return nil, nil, nil, nil
}
