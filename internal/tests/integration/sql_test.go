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
	"log"
	"os"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"
	"github.com/stretchr/testify/assert"
)

func TestIntegration_RunSQLCommand(t *testing.T) {
	// Run SQL command
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	if err != nil {
		t.Fatal(err)
	}

	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	col, err := df.Col("id")
	assert.NoError(t, err)
	df, err = df.Filter(col.Lt(functions.Lit(10)))
	assert.NoError(t, err)
	res, err = df.Collect(ctx)
	assert.NoErrorf(t, err, "Must be able to collect the rows.")
	assert.Equal(t, 10, len(res))
}

func TestIntegration_Schema(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)

	df, err := spark.Sql(ctx, "select * from range(1)")
	assert.NoError(t, err)

	schema, err := df.Schema(ctx)
	assert.NoError(t, err)

	assert.Len(t, schema.Fields, 1)
	assert.Equal(t, "id", schema.Fields[0].Name)
	assert.Equal(t, types.LongType{}, schema.Fields[0].DataType)
}

func TestMain(m *testing.M) {
	pid, err := StartSparkConnect()
	if err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	if err = StopSparkConnect(pid); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}
