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
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/column"

	"github.com/apache/spark-connect-go/v35/spark/sql/functions"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	"github.com/apache/spark-connect-go/v35/spark/sql"
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

	df, err = df.Filter(ctx, column.OfDF(df, "id").Lt(functions.IntLit(10)))
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

func TestIntegration_StructConversion(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	if err != nil {
		t.Fatal(err)
	}

	query := `
		select named_struct(
			'a', 1,
			'b', 2,
			'c', cast(10.32 as double),
			'd', array(1, 2, 3, 4)
		) struct_col
	`
	df, err := spark.Sql(ctx, query)
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))

	columnData := res[0].Values()[0]
	assert.NotNil(t, columnData)
	structDataMap, ok := columnData.(map[string]any)
	assert.True(t, ok)

	assert.Contains(t, structDataMap, "a")
	assert.Contains(t, structDataMap, "b")
	assert.Contains(t, structDataMap, "c")
	assert.Contains(t, structDataMap, "d")

	assert.Equal(t, int32(1), structDataMap["a"])
	assert.Equal(t, int32(2), structDataMap["b"])
	assert.Equal(t, float64(10.32), structDataMap["c"])
	arrayData := []any{int32(1), int32(2), int32(3), int32(4)}
	assert.Equal(t, arrayData, structDataMap["d"])

	schema, err := df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "struct_col", schema.Fields[0].Name)
}

func TestMain(m *testing.M) {
	envShouldStartService := os.Getenv("START_SPARK_CONNECT_SERVICE")
	shouldStartService := envShouldStartService == "" || envShouldStartService == "1"
	pid := int64(-1)
	var err error

	if shouldStartService {
		fmt.Println("Starting Spark Connect service...")
		pid, err = StartSparkConnect()
		if err != nil {
			log.Fatal(err)
		}
	}
	code := m.Run()
	if shouldStartService {
		if err = StopSparkConnect(pid); err != nil {
			log.Fatal(err)
		}
	}
	os.Exit(code)
}
