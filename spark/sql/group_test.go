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

package sql

import (
	"context"
	"testing"

	proto "github.com/apache/spark-connect-go/v40/internal/generated"
	"github.com/apache/spark-connect-go/v40/spark/client"
	"github.com/apache/spark-connect-go/v40/spark/client/testutils"
	"github.com/apache/spark-connect-go/v40/spark/mocks"
	"github.com/stretchr/testify/assert"
)

var sampleDataFrame = &dataFrameImpl{session: nil, relation: &proto.Relation{
	RelType: &proto.Relation_Range{
		Range: &proto.Range{
			End:  10,
			Step: 1,
		},
	},
}}

func TestGroupedData_Agg(t *testing.T) {
	ctx := context.Background()
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, mocks.AnalyzePlanResponse, nil, nil), nil, mocks.MockSessionId)
	session := sparkSessionImpl{sessionId: mocks.MockSessionId, client: c}
	sampleDataFrame.session = &session

	gd := GroupedData{
		groupType: "groupby",
		df:        sampleDataFrame,
	}

	// Should not be able to group by a non-existing column
	_, err := gd.Min(ctx, "nonExistingColumn")
	assert.Error(t, err)

	// Group by an existing column should work
	df, err := gd.Min(ctx, "col0")
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Equal(t, "min", df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions()[0].GetUnresolvedFunction().FunctionName)

	// Group by an existing column should work
	df, err = gd.Max(ctx, "col0")
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Equal(t, "max", df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions()[0].GetUnresolvedFunction().FunctionName)

	df, err = gd.Sum(ctx, "col0")
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Equal(t, "sum", df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions()[0].GetUnresolvedFunction().FunctionName)

	df, err = gd.Avg(ctx, "col0")
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Equal(t, "avg", df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions()[0].GetUnresolvedFunction().FunctionName)

	// Group by no column should pick all numeric columns
	df, err = gd.Min(ctx)
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Len(t, df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions(), 1)
}

func TestGroupedData_Count(t *testing.T) {
	ctx := context.Background()
	c := client.NewSparkExecutorFromClient(
		testutils.NewConnectServiceClientMock(nil, mocks.AnalyzePlanResponse, nil, nil), nil, mocks.MockSessionId)
	session := sparkSessionImpl{sessionId: mocks.MockSessionId, client: c}
	sampleDataFrame.session = &session

	gd := GroupedData{
		groupType: "groupby",
		df:        sampleDataFrame,
	}

	df, err := gd.Count(ctx)
	assert.NoError(t, err)
	assert.IsType(t, df.(*dataFrameImpl).relation.RelType, &proto.Relation_Aggregate{})
	assert.Equal(t, []string{"count"}, df.(*dataFrameImpl).relation.GetAggregate().GetAggregateExpressions()[0].GetAlias().Name)
}
