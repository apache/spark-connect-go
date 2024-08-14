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

package integration

import (
	"context"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntegration_BuiltinFunctions(t *testing.T) {

	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	if err != nil {
		t.Fatal(err)
	}

	df, err := spark.Sql(ctx, "select '[2]' as a from range(10)")
	df, _ = df.Filter(functions.JsonArrayLength(functions.Col("a")).Eq(functions.Lit(1)))
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(res))
}
