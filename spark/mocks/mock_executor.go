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

package mocks

import (
	"context"
	"errors"

	"github.com/apache/spark-connect-go/v35/spark/sql/utils"

	"github.com/apache/spark-connect-go/v35/spark/client/base"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

type TestExecutor struct {
	Client   base.ExecuteResponseStream
	response *generated.AnalyzePlanResponse
	Err      error
}

func (t *TestExecutor) ExecutePlan(ctx context.Context, plan *generated.Plan) (base.ExecuteResponseStream, error) {
	if t.Err != nil {
		return nil, t.Err
	}
	return t.Client, nil
}

func (t *TestExecutor) AnalyzePlan(ctx context.Context, plan *generated.Plan) (*generated.AnalyzePlanResponse, error) {
	return t.response, nil
}

func (t *TestExecutor) Explain(ctx context.Context, plan *generated.Plan,
	explainMode utils.ExplainMode,
) (*generated.AnalyzePlanResponse, error) {
	return nil, errors.New("not implemented")
}

func (t *TestExecutor) ExecuteCommand(ctx context.Context, plan *generated.Plan) (arrow.Table, *types.StructType, map[string]interface{}, error) {
	if t.Err != nil {
		return nil, nil, nil, t.Err
	}
	return nil, nil, nil, nil
}

func (t *TestExecutor) Persist(ctx context.Context, plan *generated.Plan, storageLevel utils.StorageLevel) error {
	return errors.New("not implemented")
}

func (t *TestExecutor) Unpersist(ctx context.Context, plan *generated.Plan) error {
	return errors.New("not implemented")
}

func (t *TestExecutor) GetStorageLevel(ctx context.Context, plan *generated.Plan) (*utils.StorageLevel, error) {
	return nil, errors.New("not implemented")
}

func (t *TestExecutor) SparkVersion(ctx context.Context) (string, error) {
	return "", errors.New("not implemented")
}

func (t *TestExecutor) DDLParse(ctx context.Context, sql string) (*types.StructType, error) {
	return nil, errors.New("not implemented")
}

func (t *TestExecutor) SameSemantics(ctx context.Context, plan1 *generated.Plan, plan2 *generated.Plan) (bool, error) {
	return false, errors.New("not implemented")
}

func (t *TestExecutor) SemanticHash(ctx context.Context, plan *generated.Plan) (int32, error) {
	return 0, errors.New("not implemented")
}
