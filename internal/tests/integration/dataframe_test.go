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
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	"github.com/apache/spark-connect-go/v35/spark/sql/column"

	"github.com/apache/spark-connect-go/v35/spark/sql/functions"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/stretchr/testify/assert"
)

func TestDataFrame_Select(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	df, err = df.Select(ctx, functions.Lit("1"), functions.Lit("2"))
	assert.NoError(t, err)

	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	row_zero := res[0]
	vals, err := row_zero.Values()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(vals))

	df, err = spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	_, err = df.Select(ctx, column.OfDF(df, "id2"))
	assert.Error(t, err)
}

func TestDataFrame_SelectExpr(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	df, err = df.SelectExpr(ctx, "1", "2", "spark_partition_id()")
	assert.NoError(t, err)

	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	row_zero := res[0]
	vals, err := row_zero.Values()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
}

func TestDataFrame_Alias(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	df = df.Alias(ctx, "df")
	res, er := df.Collect(ctx)
	assert.NoError(t, er)
	assert.Equal(t, 100, len(res))
}

func TestDataFrame_CrossJoin(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)
	df1, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df2, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df := df1.CrossJoin(ctx, df2)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	v, e := res[0].Values()
	assert.NoError(t, e)
	assert.Equal(t, 2, len(v))
}

func TestDataFrame_GroupBy(t *testing.T) {
	ctx, spark := connect()
	src, _ := spark.Sql(ctx, "select 'a' as a, 1 as b from range(10)")
	df, _ := src.GroupBy(functions.Col("a")).Agg(ctx, functions.Sum(functions.Col("b")))

	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))

	df, err = src.GroupBy(functions.Col("a")).Count(ctx)
	assert.NoError(t, err)
	res, err = df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	vals, _ := res[0].Values()
	assert.Equal(t, "a", vals[0])
	assert.Equal(t, int64(10), vals[1])
}

func TestDataFrame_Count(t *testing.T) {
	ctx, spark := connect()
	src, _ := spark.Sql(ctx, "select 'a' as a, 1 as b from range(10)")
	res, err := src.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), res)
}

func TestDataFrame_OfDFWithRegex(t *testing.T) {
	ctx, spark := connect()
	src, _ := spark.Sql(ctx, "select 'a' as myColumnName, 1 as b from range(10)")
	col := column.OfDFWithRegex(src, "`.*(Column).*`")
	res, err := src.Select(ctx, col)
	assert.NoError(t, err)
	schema, err := res.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
}

func TestSparkSession_CreateDataFrame(t *testing.T) {
	ctx, spark := connect()

	tbl := createArrowTable()
	defer tbl.Release()

	df, err := spark.CreateDataFrameFromArrow(ctx, tbl)
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(res))
}

func TestSparkSession_CreateDataFrameWithSchema(t *testing.T) {
	ctx, spark := connect()

	data := [][]any{
		{1, 1.1, "a"},
		{2, 2.2, "b"},
	}
	schema := types.StructOf(
		types.NewStructField("f1-i32", types.INTEGER),
		types.NewStructField("f2-f64", types.DOUBLE),
		types.NewStructField("f3-string", types.STRING))

	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))

	row1, err := res[0].Values()
	assert.NoError(t, err)
	assert.Len(t, row1, 3)
	assert.Equal(t, int32(1), row1[0])
	assert.Equal(t, 1.1, row1[1])
	assert.Equal(t, "a", row1[2])
}

func TestDataFrame_Corr(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{
		{1, 12}, {10, 1}, {19, 8},
	}
	schema := types.StructOf(
		types.NewStructField("c1", types.INTEGER),
		types.NewStructField("c2", types.INTEGER),
	)

	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	res, err := df.Corr(ctx, "c1", "c2")
	assert.NoError(t, err)
	assert.Equal(t, -0.3592106040535498, res)
}

func TestDataFrame_Cov(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{
		{1, 12}, {10, 1}, {19, 8},
	}
	schema := types.StructOf(
		types.NewStructField("c1", types.INTEGER),
		types.NewStructField("c2", types.INTEGER),
	)

	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	res, err := df.Cov(ctx, "c1", "c2")
	assert.NoError(t, err)
	assert.Equal(t, -18.0, res)
}

func TestDataFrame_WithColumn(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithColumn(ctx, "newCol", functions.Lit(1))
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(res))
	// Check the values of the new column
	for _, row := range res {
		vals, err := row.Values()
		assert.NoError(t, err)
		assert.Equal(t, 2, len(vals))
		assert.Equal(t, int64(1), vals[1])
	}
}

func TestDataFrame_WithColumns(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithColumns(ctx, map[string]column.Convertible{
		"newCol1": functions.Lit(1),
		"newCol2": functions.Lit(2),
	})
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(res))
	// Check the values of the new columns
	for _, row := range res {
		vals, err := row.Values()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(vals))
		assert.Equal(t, int64(1), vals[1])
		assert.Equal(t, int64(2), vals[2])
	}
}

func TestDataFrame_WithMetadata(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithMetadata(ctx, map[string]string{"id": "value"})
	assert.NoError(t, err)
	_, err = df.Schema(ctx)
	assert.Error(t, err, "Expecting malformed metadata")

	df, err = spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithMetadata(ctx, map[string]string{"id": "{\"kk\": \"value\"}"})
	assert.NoError(t, err)
	schema, err := df.Schema(ctx)
	assert.NoError(t, err)
	fields := schema.Fields[0]
	assert.Equal(t, "id", fields.Name)
	assert.Equal(t, "{\"kk\":\"value\"}", *fields.Metadata)
}

func TestDataFrame_WithColumnRenamed(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithColumnRenamed(ctx, "id", "newId")
	assert.NoError(t, err)
	// Check the schema of the new dataframe
	schema, err := df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "newId", schema.Fields[0].Name)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(res))
	// Check the values of the new column
	for i, row := range res {
		vals, err := row.Values()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(vals))
		assert.Equal(t, int64(i), vals[0])
	}

	// Test that renaming a non-existing column does not change anything.
	df, _ = spark.Sql(ctx, "select * from range(10)")
	df, err = df.WithColumnRenamed(ctx, "nonExisting", "newId")
	assert.NoError(t, err)
	schema, err = df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "id", schema.Fields[0].Name)

	// Test that single column renaming works as well.
	df, _ = spark.Sql(ctx, "select * from range(10)")
	df, err = df.WithColumnRenamed(ctx, "id", "newId")
	assert.NoError(t, err)
	schema, err = df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "newId", schema.Fields[0].Name)
}

func TestDataFrame_WithWatermark(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select current_timestamp() as this_time from range(10)")
	assert.NoError(t, err)
	df, err = df.WithWatermark(ctx, "this_time", "1 minute")
	assert.NoError(t, err)
	schema, err := df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "this_time", schema.Fields[0].Name)
}

func TestDataFrame_Where(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.Where(ctx, "id = 0")
	assert.NoError(t, err)
	res, err := df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
}

func TestDataFrame_Drop(t *testing.T) {
	ctx, spark := connect()
	src, err := spark.Sql(ctx, "select 1 as id, 2 as other from range(10)")
	assert.NoError(t, err)
	df, err := src.DropByName(ctx, "id")
	assert.NoError(t, err)
	schema, err := df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "other", schema.Fields[0].Name)

	df, err = src.Drop(ctx, column.OfDF(src, "other"))
	assert.NoError(t, err)
	schema, err = df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(schema.Fields))
	assert.Equal(t, "id", schema.Fields[0].Name)
}

func TestDataFrame_DropDuplicates(t *testing.T) {
	ctx, spark := connect()
	src, err := spark.Sql(ctx, "select 1 as id, 2 as other from range(10)")
	assert.NoError(t, err)
	df, err := src.DropDuplicates(ctx)
	assert.NoError(t, err)
	res, err := df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)

	// Create a dataframe with duplicate rows
	data := [][]any{
		{"Alice", 5, 80}, {"Alice", 5, 80}, {"Alice", 10, 80},
	}
	schema := types.StructOf(
		types.NewStructField("name", types.STRING),
		types.NewStructField("age", types.INTEGER),
		types.NewStructField("height", types.INTEGER),
	)

	df, err = spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	// Check the schema of the dataframe
	schema, err = df.Schema(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(schema.Fields))
	assert.Equal(t, "name", schema.Fields[0].Name)
	assert.Equal(t, "age", schema.Fields[1].Name)
	assert.Equal(t, "height", schema.Fields[2].Name)

	df, err = df.DropDuplicates(ctx)
	assert.NoError(t, err)
	res, err = df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res)
	// Check the two ages
	rows, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rows))
	vals, _ := rows[0].Values()
	assert.Equal(t, "Alice", vals[0])
	assert.Equal(t, int32(5), vals[1])
	vals, _ = rows[1].Values()
	assert.Equal(t, "Alice", vals[0])
	assert.Equal(t, int32(10), vals[1])

	// Test drop duplicates with column names
	df, err = df.DropDuplicates(ctx, "name")
	assert.NoError(t, err)
	res, err = df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
}
