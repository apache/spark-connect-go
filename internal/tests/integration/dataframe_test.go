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
	"os"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/utils"

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

	rowZero := res[0]
	assert.Equal(t, 2, rowZero.Len())

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
	assert.Equal(t, 3, row_zero.Len())
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
	assert.Equal(t, 2, res[0].Len())
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
	assert.Equal(t, "a", res[0].At(0))
	assert.Equal(t, int64(10), res[0].At(1))
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
	assert.Equal(t, 3, res[0].Len())
	assert.Equal(t, int32(1), res[0].At(0))
	assert.Equal(t, 1.1, res[0].At(1))
	assert.Equal(t, "a", res[0].At(2))
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
		assert.Equal(t, 2, row.Len())
		assert.Equal(t, int64(1), row.At(1))
	}
}

func TestDataFrame_WithColumns(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithColumns(ctx, column.WithAlias("newCol1", functions.Lit(1)),
		column.WithAlias("newCol2", functions.Lit(2)))
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(res))
	// Check the values of the new columns
	for _, row := range res {
		assert.Equal(t, 3, row.Len())
		assert.Equal(t, int64(1), row.At(1))
		assert.Equal(t, int64(2), row.At(2))
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
		assert.Equal(t, 1, row.Len())
		assert.Equal(t, int64(i), row.At(0))
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
	assert.Equal(t, "Alice", rows[0].At(0))
	assert.Equal(t, int32(5), rows[0].At(1))
	assert.Equal(t, "Alice", rows[1].At(0))
	assert.Equal(t, int32(10), rows[1].At(1))

	// Test drop duplicates with column names
	df, err = df.DropDuplicates(ctx, "name")
	assert.NoError(t, err)
	res, err = df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res)
}

func TestDataFrame_Explain(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	res, err := df.Explain(ctx, utils.ExplainModeSimple)
	assert.NoError(t, err)
	assert.Contains(t, res, "Physical Plan")

	res, err = df.Explain(ctx, utils.ExplainModeExtended)
	assert.NoError(t, err)
	assert.Contains(t, res, "Physical Plan")

	res, err = df.Explain(ctx, utils.ExplainModeCodegen)
	assert.NoError(t, err)
	assert.Contains(t, res, "WholeStageCodegen")

	res, err = df.Explain(ctx, utils.ExplainModeCost)
	assert.NoError(t, err)
	assert.Contains(t, res, "Physical Plan")

	res, err = df.Explain(ctx, utils.ExplainModeFormatted)
	assert.NoError(t, err)
	assert.Contains(t, res, "Physical Plan")
}

func TestDataFrame_CachingAndPersistence(t *testing.T) {
	ctx, spark := connect()
	levels := []utils.StorageLevel{
		utils.StorageLevelDiskOnly,
		utils.StorageLevelDiskOnly2,
		utils.StorageLevelDiskOnly3,
		utils.StorageLevelMemoryAndDisk,
		utils.StorageLevelMemoryAndDisk2,
		utils.StorageLevelMemoryOnly,
		utils.StorageLevelMemoryOnly2,
		utils.StorageLevelMemoyAndDiskDeser,
		utils.StorageLevelOffHeap,
	}

	for _, lvl := range levels {
		df, err := spark.Sql(ctx, "select * from range(10)")
		assert.NoError(t, err)
		err = df.Persist(ctx, lvl)
		assert.NoError(t, err)
		l, err := df.GetStorageLevel(ctx)
		assert.NoError(t, err)

		assert.Contains(t, []utils.StorageLevel{lvl, utils.StorageLevelMemoryOnly}, *l)

		err = df.Unpersist(ctx)
		assert.NoError(t, err)
	}
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	err = df.Cache(ctx)
	assert.NoError(t, err)
	l, err := df.GetStorageLevel(ctx)
	assert.NoError(t, err)
	assert.Equal(t, utils.StorageLevelMemoryOnly, *l, "%v != %v", utils.StorageLevelMemoryOnly, *l)
}

func TestDataFrame_SetOps(t *testing.T) {
	ctx, spark := connect()
	df1, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df2, err := spark.Sql(ctx, "select * from range(5)")
	assert.NoError(t, err)

	df := df1.Union(ctx, df2)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 15, len(res))

	df = df1.Intersect(ctx, df2)
	res, err = df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(res))

	df = df1.ExceptAll(ctx, df2)
	res, err = df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(res))
}

func TestDataFrame_ToArrow(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	tbl, err := df.ToArrow(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, tbl)
}

func TestDataFrame_LimitVersions(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df = df.Limit(ctx, int32(5))
	assert.NoError(t, err)
	rows, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 5)

	rows, err = df.Tail(ctx, int32(3))
	assert.NoError(t, err)
	assert.Len(t, rows, 3)

	rows, err = df.Head(ctx, int32(3))
	assert.NoError(t, err)
	assert.Len(t, rows, 3)

	rows, err = df.Take(ctx, int32(3))
	assert.NoError(t, err)
	assert.Len(t, rows, 3)
}

func TestDataFrame_Sort(t *testing.T) {
	ctx, spark := connect()
	src, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err := src.Sort(ctx, functions.Col("id").Desc())
	assert.NoError(t, err)
	res, err := df.Head(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(9), res[0].At(0))

	df, err = src.Sort(ctx, functions.Col("id").Asc())
	assert.NoError(t, err)
	res, err = df.Head(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), res[0].At(0))
}

func TestDataFrame_Join(t *testing.T) {
	ctx, spark := connect()
	df1, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df2, err := spark.Sql(ctx, "select * from range(5)")
	assert.NoError(t, err)

	df, err := df1.Join(ctx, df2, column.OfDF(df1, "id").Eq(column.OfDF(df2, "id")), utils.JoinTypeInner)
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(res))
}

func TestDataFrame_RandomSplits(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(1000)")
	assert.NoError(t, err)
	dfs, err := df.RandomSplit(ctx, []float64{0.3, 0.7})
	assert.NoError(t, err)
	assert.Len(t, dfs, 2)
	c1, err := dfs[0].Count(ctx)
	assert.NoError(t, err)
	c2, err := dfs[1].Count(ctx)
	assert.NoError(t, err)
	assert.Less(t, c1, c2)
}

func TestDataFrame_Describe(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	res, err := df.Describe(ctx, "id").Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 5)
}

func TestDataFrame_Summary(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select id, 'a' as col, 2 as other from range(10)")
	assert.NoError(t, err)
	res, err := df.Summary(ctx, "count", "stddev").Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Equal(t, "count", res[0].At(0))
	assert.Equal(t, 4, res[0].Len())
}

func TestDataFrame_Pivot(t *testing.T) {
	ctx, spark := connect()

	data := [][]any{
		{"dotNET", 2012, 10000},
		{"Java", 2012, 20000},
		{"dotNET", 2012, 5000},
		{"dotNET", 2013, 48000},
		{"Java", 2013, 30000},
	}
	schema := types.StructOf(
		types.NewStructField("course", types.STRING),
		types.NewStructField("year", types.INTEGER),
		types.NewStructField("earnings", types.INTEGER))

	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	gd := df.GroupBy(functions.Col("year"))
	gd, err = gd.Pivot(ctx, "course", []any{"Java", "dotNET"})
	assert.NoError(t, err)
	df, err = gd.Sum(ctx, "earnings")
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
}

func TestDataFrame_Offset(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df = df.Offset(ctx, int32(5))
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 5)
}

func TestDataFrame_IsEmpty(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	empty, err := df.IsEmpty(ctx)
	assert.NoError(t, err)
	assert.False(t, empty)

	df, err = spark.Sql(ctx, "select * from range(0)")
	assert.NoError(t, err)
	empty, err = df.IsEmpty(ctx)
	assert.NoError(t, err)
	assert.True(t, empty)
}

func TestDataFrame_First(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	row, err := df.First(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), row.At(0))
}

func TestDataFrame_Distinct(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df = df.Distinct(ctx)
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 10)
}

func TestDataFrame_CrossTab(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{{1, 11}, {1, 11}, {3, 10}, {4, 8}, {4, 8}}
	schema := types.StructOf(
		types.NewStructField("c1", types.INTEGER),
		types.NewStructField("c2", types.INTEGER),
	)

	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	df = df.CrossTab(ctx, "c1", "c2")
	df, err = df.Sort(ctx, column.OfDF(df, "c1_c2").Asc())
	assert.NoError(t, err)
	res, err := df.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, res, 3)
	assert.Equal(t, "1", res[0].At(0))
	assert.Equal(t, int64(0), res[0].At(1))
	assert.Equal(t, int64(2), res[0].At(2))
	assert.Equal(t, int64(0), res[0].At(3))
}

func TestDataFrame_SameSemantics(t *testing.T) {
	ctx, spark := connect()
	df1, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df2, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	res, _ := df1.SameSemantics(ctx, df2)
	assert.True(t, res)
}

func TestDataFrame_SemanticHash(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	hash, err := df.SemanticHash(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, hash)
}

func TestDataFrame_FreqItems(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select id % 4 as id from range(100)")
	assert.NoError(t, err)
	res, err := df.FreqItems(ctx, "id").Collect(ctx)
	assert.NoErrorf(t, err, "%+v", err)
	assert.Len(t, res, 1)
}

func TestDataFrame_WithOption(t *testing.T) {
	ctx, spark := connect()
	file, err := os.CreateTemp("", "example")
	defer os.Remove(file.Name())
	assert.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString("id#name,name\n")
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = file.WriteString(fmt.Sprintf("%d#alice,alice\n", i))
		assert.NoError(t, err)
	}
	df, err := spark.Read().Format("csv").
		Option("header", "true").
		Option("quote", "\"").
		Option("sep", "#").
		Option("escapeQuotes", "true").
		// Option("skipLines", "5"). //TODO: this needs more insight
		Option("inferSchema", "false").
		Load(file.Name())
	assert.NoError(t, err)
	c, err := df.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), c)
}

func TestDataFrame_Sample(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	testCases := []struct {
		name     string
		fraction float64
	}{
		{
			name:     "Default behavior",
			fraction: 0.1,
		},
		{
			name:     "Large fraction",
			fraction: 0.9,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sampledDF, err := df.Sample(ctx, tc.fraction)
			assert.NoError(t, err)
			count, err := sampledDF.Count(ctx)
			assert.NoError(t, err)
			expectedSize := int(100 * tc.fraction)
			assert.InDelta(t, expectedSize, count, float64(expectedSize), 10)
		})
	}
}

func TestDataFrame_SampleWithReplacement(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	testCases := []struct {
		name            string
		withReplacement bool
		fraction        float64
	}{
		{
			name:            "With replacement",
			withReplacement: true,
			fraction:        0.1,
		},
		{
			name:            "Without replacement",
			withReplacement: false,
			fraction:        0.1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sampledDF, err := df.SampleWithReplacement(ctx, tc.withReplacement, tc.fraction)
			assert.NoError(t, err)
			count, err := sampledDF.Count(ctx)
			assert.NoError(t, err)
			expectedSize := int(100 * tc.fraction)
			assert.InDelta(t, expectedSize, count, float64(expectedSize), 10)
			rows, err := sampledDF.Collect(ctx)
			assert.NoError(t, err)
			// If sampling without replacement, check for duplicates
			if tc.withReplacement == false {
				seen := make(map[int64]bool)
				for _, row := range rows {
					value := row.At(0).(int64)
					if seen[value] {
						t.Fatal("Found duplicate value when sampling without replacement")
					}
					seen[value] = true
				}
			}
		})
	}
}

func TestDataFrame_SampleWithSeed(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	fraction := 0.1
	seed := int64(17)
	sampledDF, err := df.SampleSeed(ctx, fraction, seed)
	assert.NoError(t, err)
	count, err := sampledDF.Count(ctx)
	assert.NoError(t, err)
	expectedSize := int(100 * fraction)
	assert.InDelta(t, expectedSize, count, float64(expectedSize), 10)
	rows, err := sampledDF.Collect(ctx)
	assert.NoError(t, err)
	// If sampling without replacement, check for duplicates
	seen := make(map[int64]bool)
	for _, row := range rows {
		value := row.At(0).(int64)
		if seen[value] {
			t.Fatal("Found duplicate value when sampling without replacement")
		}
		seen[value] = true
	}
	// same seed should return same output
	sampledDFRepeat, err := df.SampleSeed(ctx, fraction, seed)
	assert.NoError(t, err)
	count2, err := sampledDFRepeat.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
	rows2, err := sampledDFRepeat.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, rows, rows2)
}

func TestDataFrame_SampleWithReplacementWithSeed(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	fraction := 0.1
	seed := int64(17)
	sampledDF, err := df.SampleWithReplacementSeed(ctx, true, fraction, seed)
	assert.NoError(t, err)
	count, err := sampledDF.Count(ctx)
	assert.NoError(t, err)
	expectedSize := int(100 * fraction)
	assert.InDelta(t, expectedSize, count, float64(expectedSize), 10)
	rows, err := sampledDF.Collect(ctx)
	assert.NoError(t, err)
	// same seed should return same output
	sampledDFRepeat, err := df.SampleWithReplacementSeed(ctx, true, fraction, seed)
	assert.NoError(t, err)
	count2, err := sampledDFRepeat.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
	rows2, err := sampledDFRepeat.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, rows, rows2)
}
