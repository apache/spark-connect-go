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
	"testing"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"
	"github.com/stretchr/testify/assert"
)

func TestDataFrameImpl_GroupBy(t *testing.T) {
	ctx := context.Background()
	rel := &proto.Relation{
		RelType: &proto.Relation_Range{
			Range: &proto.Range{
				End:  10,
				Step: 1,
			},
		},
	}
	df := NewDataFrame(nil, rel)
	gd := df.GroupBy(functions.Col("id"))
	assert.NotNil(t, gd)

	assert.Equal(t, gd.groupType, "groupby")

	df, err := gd.Agg(ctx, functions.Count(functions.Int64Lit(1)))
	assert.Nil(t, err)
	impl := df.(*dataFrameImpl)
	assert.NotNil(t, impl)
	assert.IsType(t, impl.relation.RelType, &proto.Relation_Aggregate{})
}
