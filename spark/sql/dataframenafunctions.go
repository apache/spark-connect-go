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

	"github.com/apache/spark-connect-go/v40/spark/sql/types"
)

type DataFrameNaFunctions interface {
	Drop(ctx context.Context, cols ...string) (DataFrame, error)
	DropAll(ctx context.Context, cols ...string) (DataFrame, error)
	DropWithThreshold(ctx context.Context, threshold int32, cols ...string) (DataFrame, error)
	Fill(ctx context.Context, value types.PrimitiveTypeLiteral, cols ...string) (DataFrame, error)
	FillWithValues(ctx context.Context, values map[string]types.PrimitiveTypeLiteral) (DataFrame, error)
	Replace(ctx context.Context, toReplace []types.PrimitiveTypeLiteral,
		values []types.PrimitiveTypeLiteral, cols ...string) (DataFrame, error)
}

type dataFrameNaFunctionsImpl struct {
	dataFrame DataFrame
}

func (d *dataFrameNaFunctionsImpl) Drop(ctx context.Context, cols ...string) (DataFrame, error) {
	return d.dataFrame.DropNa(ctx, cols...)
}

func (d *dataFrameNaFunctionsImpl) DropAll(ctx context.Context, cols ...string) (DataFrame, error) {
	return d.dataFrame.DropNaAll(ctx, cols...)
}

func (d *dataFrameNaFunctionsImpl) DropWithThreshold(ctx context.Context, threshold int32, cols ...string) (DataFrame, error) {
	return d.dataFrame.DropNaWithThreshold(ctx, threshold, cols...)
}

func (d *dataFrameNaFunctionsImpl) Fill(ctx context.Context, value types.PrimitiveTypeLiteral, cols ...string) (DataFrame, error) {
	return d.dataFrame.FillNa(ctx, value, cols...)
}

func (d *dataFrameNaFunctionsImpl) FillWithValues(ctx context.Context,
	values map[string]types.PrimitiveTypeLiteral,
) (DataFrame, error) {
	return d.dataFrame.FillNaWithValues(ctx, values)
}

func (d *dataFrameNaFunctionsImpl) Replace(ctx context.Context,
	toReplace []types.PrimitiveTypeLiteral, values []types.PrimitiveTypeLiteral, cols ...string,
) (DataFrame, error) {
	return d.dataFrame.Replace(ctx, toReplace, values, cols...)
}
