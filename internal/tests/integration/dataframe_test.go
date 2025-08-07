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

	"github.com/apache/spark-connect-go/spark/sql/utils"

	"github.com/apache/spark-connect-go/spark/sql/types"

	"github.com/apache/spark-connect-go/spark/sql/column"

	"github.com/apache/spark-connect-go/spark/sql/functions"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataFrame_Select(t *testing.T) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	assert.NoError(t, err)
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	df, err = df.Select(ctx, functions.StringLit("1"), functions.StringLit("2"))
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

	res2, err := df.Stat().Corr(ctx, "c1", "c2")
	assert.NoError(t, err)
	assert.Equal(t, res, res2)
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

	res2, err := df.Stat().Cov(ctx, "c1", "c2")
	assert.NoError(t, err)
	assert.Equal(t, res, res2)
}

func TestDataFrame_WithColumn(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	df, err = df.WithColumn(ctx, "newCol", functions.IntLit(1))
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
	df, err = df.WithColumns(ctx, column.WithAlias("newCol1", functions.IntLit(1)),
		column.WithAlias("newCol2", functions.IntLit(2)))
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
	gd, err = gd.Pivot(ctx, "course", []types.LiteralType{types.String("Java"), types.String("dotNET")})
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

	df, err = spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	df = df.Stat().CrossTab(ctx, "c1", "c2")
	df, err = df.Sort(ctx, column.OfDF(df, "c1_c2").Asc())
	assert.NoError(t, err)
	res2, err := df.Collect(ctx)
	assert.NoError(t, err)

	assert.Equal(t, res, res2)
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

	res2, err := df.Stat().FreqItems(ctx, "id").Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, res, res2)
}

func TestDataFrame_Config_GetAll(t *testing.T) {
	ctx, spark := connect()
	result, err := spark.Config().GetAll(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "driver", result["spark.executor.id"])
}

func TestDataFrame_Config_Get(t *testing.T) {
	ctx, spark := connect()
	result, err := spark.Config().Get(ctx, "spark.executor.id")
	assert.NoError(t, err)
	assert.Equal(t, "driver", result)
}

func TestDataFrame_Config_GetWithDefault(t *testing.T) {
	ctx, spark := connect()

	result, err := spark.Config().GetWithDefault(ctx, "spark.whatever", "whatever_not_set")
	assert.NoError(t, err)
	assert.Equal(t, "whatever_not_set", result)
}

func TestDataFrame_Config_Set(t *testing.T) {
	ctx, spark := connect()
	err := spark.Config().Set(ctx, "spark.whatever", "whatever_set")
	assert.NoError(t, err)
}

func TestDataFrame_Config_IsModifiable(t *testing.T) {
	ctx, spark := connect()
	result, err := spark.Config().IsModifiable(ctx, "spark.executor.id")
	assert.NoError(t, err)
	assert.Equal(t, false, result)
}

func TestDataFrame_Config_Unset(t *testing.T) {
	ctx, spark := connect()
	err := spark.Config().Set(ctx, "spark.whatever", "whatever_set")
	assert.NoError(t, err)
	err = spark.Config().Unset(ctx, "spark.whatever")
	assert.NoError(t, err)
}

func TestDataFrame_Config_e2e_test(t *testing.T) {
	ctx, spark := connect()
	//  add keys that we know is "modifiable"
	key := "spark.sql.ansi.enabled"
	result, err := spark.Config().IsModifiable(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, true, result)
	_, err = spark.Config().Get(ctx, key)
	assert.NoError(t, err)
	err = spark.Config().Set(ctx, "spark.sql.ansi.enabled", "true")
	assert.NoError(t, err)
	m, err := spark.Config().Get(ctx, "spark.sql.ansi.enabled")
	assert.NoError(t, err)
	assert.Equal(t, "true", m)
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

func TestDataFrame_SampleSeed(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	fraction := 0.1
	seed := int64(17)
	sampledDF, err := df.SampleWithSeed(ctx, fraction, seed)
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
	sampledDFRepeat, err := df.SampleWithSeed(ctx, fraction, seed)
	assert.NoError(t, err)
	count2, err := sampledDFRepeat.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
	rows2, err := sampledDFRepeat.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, rows, rows2)
}

func TestDataFrame_SampleWithReplacementSeed(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(100)")
	assert.NoError(t, err)
	fraction := 0.1
	seed := int64(17)
	sampledDF, err := df.SampleWithReplacementAndSeed(ctx, true, fraction, seed)
	assert.NoError(t, err)
	count, err := sampledDF.Count(ctx)
	assert.NoError(t, err)
	expectedSize := int(100 * fraction)
	assert.InDelta(t, expectedSize, count, float64(expectedSize), 10)
	rows, err := sampledDF.Collect(ctx)
	assert.NoError(t, err)
	// same seed should return same output
	sampledDFRepeat, err := df.SampleWithReplacementAndSeed(ctx, true, fraction, seed)
	assert.NoError(t, err)
	count2, err := sampledDFRepeat.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, count, count2)
	rows2, err := sampledDFRepeat.Collect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, rows, rows2)
}

func TestDataFrame_Unpivot(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{{1, 11, 1.1}, {2, 12, 1.2}}
	schema := types.StructOf(
		types.NewStructField("id", types.INTEGER),
		types.NewStructField("int", types.INTEGER),
		types.NewStructField("double", types.DOUBLE),
	)
	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)

	udf, err := df.Unpivot(ctx, []column.Convertible{functions.Col("id")},
		[]column.Convertible{functions.Col("int"), functions.Col("double")},
		"type", "value")

	assert.NoError(t, err)
	cnt, err := udf.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), cnt)
}

func TestDataFrame_Replace(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{
		{10, 80, "Alice"},
		{5, nil, "Bob"},
		{nil, 10, "Tom"},
		{nil, nil, nil},
	}
	schema := types.StructOf(
		types.NewStructField("age", types.INTEGER),
		types.NewStructField("height", types.INTEGER),
		types.NewStructField("name", types.STRING),
	)
	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)

	res, err := df.Replace(ctx,
		[]types.PrimitiveTypeLiteral{types.Int32(10)},
		[]types.PrimitiveTypeLiteral{types.Int32(20)},
	)
	assert.NoError(t, err)

	cnt, err := res.Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), cnt)

	rows, err := res.Collect(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int32(20), rows[0].At(0))
	assert.Equal(t, int32(20), rows[2].At(1))

	res, err = df.Replace(ctx,
		[]types.PrimitiveTypeLiteral{types.Int32(10)},
		[]types.PrimitiveTypeLiteral{types.Int32Nil},
	)
	assert.NoError(t, err)

	rows, err = res.Collect(ctx)
	assert.NoError(t, err)
	assert.Nil(t, rows[0].At(0))
}

func TestDataFrame_ReplaceWithColumn(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{
		{10, 80, "Alice"},
		{5, nil, "Bob"},
		{nil, 10, "Tom"},
		{nil, nil, nil},
	}
	schema := types.StructOf(
		types.NewStructField("age", types.INTEGER),
		types.NewStructField("height", types.INTEGER),
		types.NewStructField("name", types.STRING),
	)
	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)

	res, err := df.Replace(ctx, []types.PrimitiveTypeLiteral{types.Int32(10)},
		[]types.PrimitiveTypeLiteral{types.Int32(20)}, "age")
	assert.NoError(t, err)

	rows, err := res.Collect(ctx)
	assert.NoError(t, err)
	// Should only repalce the age column but not the height column
	assert.Equal(t, int32(20), rows[0].At(0))
	assert.Equal(t, int32(10), rows[2].At(1))
}

func TestDataFrame_FillNa(t *testing.T) {
	ctx, spark := connect()
	file, err := os.CreateTemp("", "fillna")
	defer os.Remove(file.Name())
	assert.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(`{"id":1,"int":null, "int2": 1}
{"id":null,"int":12, "int2": null}
`)
	assert.NoError(t, err)

	df, err := spark.Read().Format("json").
		Option("inferSchema", "true").
		Load(file.Name())
	assert.NoError(t, err)

	// all columns
	filled, err := df.FillNa(ctx, types.Int64(10))
	assert.NoError(t, err)
	sorted, err := filled.Sort(ctx, functions.Col("id").Asc())
	assert.NoError(t, err)
	res, err := sorted.Collect(ctx)
	assert.NoError(t, err)
	require.Equal(t, 2, len(res))
	assert.Equal(t, []any{int64(1), int64(10), int64(1)}, res[0].Values())
	assert.Equal(t, []any{int64(10), int64(12), int64(10)}, res[1].Values())

	// specific columns
	filled, err = df.FillNa(ctx, types.Int64(10), "int", "int2")
	assert.NoError(t, err)
	sorted, err = filled.Sort(ctx, functions.Col("id").Asc())
	assert.NoError(t, err)
	res, err = sorted.Collect(ctx)
	assert.NoError(t, err)
	require.Equal(t, 2, len(res))
	assert.Equal(t, []any{nil, int64(12), int64(10)}, res[0].Values())
	assert.Equal(t, []any{int64(1), int64(10), int64(1)}, res[1].Values())

	// specific columns with map
	filled, err = df.FillNaWithValues(ctx, map[string]types.PrimitiveTypeLiteral{
		"int": types.Int64(10), "int2": types.Int64(20),
	})
	assert.NoError(t, err)
	sorted, err = filled.Sort(ctx, functions.Col("id").Asc())
	assert.NoError(t, err)
	res, err = sorted.Collect(ctx)
	assert.NoError(t, err)
	require.Equal(t, 2, len(res))
	assert.Equal(t, []any{nil, int64(12), int64(20)}, res[0].Values())
	assert.Equal(t, []any{int64(1), int64(10), int64(1)}, res[1].Values())
}

func TestDataFrame_ApproxQuantile(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select id, 1 as id2 from range(100)")
	assert.NoError(t, err)
	res, err := df.ApproxQuantile(ctx, []float64{float64(0.5)}, float64(0.1), "id")
	assert.NoError(t, err)
	assert.Len(t, res, 1)

	data := [][]any{
		{"bob", "Developer", 125000, 1},
		{"mark", "Developer", 108000, 2},
		{"carl", "Tester", 70000, 2},
		{"peter", "Developer", 185000, 2},
		{"jon", "Tester", 65000, 1},
		{"roman", "Tester", 82000, 2},
		{"simon", "Developer", 98000, 1},
		{"eric", "Developer", 144000, 2},
		{"carlos", "Tester", 75000, 1},
		{"henry", "Developer", 110000, 1},
	}
	schema := types.StructOf(
		types.NewStructField("Name", types.STRING),
		types.NewStructField("Role", types.STRING),
		types.NewStructField("Salary", types.LONG),
		types.NewStructField("Performance", types.LONG),
	)

	df, err = spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)
	med, err := df.ApproxQuantile(ctx, []float64{float64(0.5)}, float64(0.25), "Salary")

	assert.NoError(t, err)
	assert.Len(t, med, 1)
	assert.GreaterOrEqual(t, med[0][0], 75000.0)

	_, err = df.Stat().ApproxQuantile(ctx, []float64{0.5}, 0.25, "Salary")
	assert.NoError(t, err)
}

func TestDataFrame_DFNaFunctions(t *testing.T) {
	ctx, spark := connect()
	data := [][]any{
		{10, 80.5, "Alice", true},
		{5, nil, "Bob", nil},
		{nil, nil, "Tom", nil},
		{nil, nil, nil, nil},
	}
	schema := types.StructOf(
		types.NewStructField("age", types.INTEGER),
		types.NewStructField("height", types.DOUBLE),
		types.NewStructField("name", types.STRING),
		types.NewStructField("bool", types.BOOLEAN),
	)
	df, err := spark.CreateDataFrame(ctx, data, schema)
	assert.NoError(t, err)

	res, err := df.Na().Drop(ctx)
	assert.NoError(t, err)
	rows, err := res.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, rows[0].At(2), "Alice")

	res, err = df.Na().DropAll(ctx)
	assert.NoError(t, err)
	rows, err = res.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 3)

	// Fill must only use long types
	res, err = df.Na().Fill(ctx, types.Int64(50))
	assert.NoError(t, err)
	rows, err = res.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 4)

	assert.Equal(t, int32(50), rows[2].At(0))
	assert.Equal(t, int32(50), rows[3].At(0))
	assert.Equal(t, float64(50), rows[2].At(1))
	assert.Equal(t, float64(50), rows[3].At(1))

	res, err = df.Na().Replace(ctx, []types.PrimitiveTypeLiteral{types.String("Alice")}, []types.PrimitiveTypeLiteral{
		types.String("Bob"),
	})
	assert.NoError(t, err)
	rows, err = res.Collect(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 4)

	assert.Equal(t, "Bob", rows[0].At(2))
}

func TestDataFrame_RangeIter(t *testing.T) {
	ctx, spark := connect()
	df, err := spark.Sql(ctx, "select * from range(10)")
	assert.NoError(t, err)
	cnt := 0
	for row, err := range df.All(ctx) {
		assert.NoError(t, err)
		assert.NotNil(t, row)
		cnt++
	}
	assert.Equal(t, 10, cnt)

	// Check that errors are properly propagated
	df, err = spark.Sql(ctx, "select if(id = 5, raise_error('handle'), false) from range(10)")
	assert.NoError(t, err)
	for _, err := range df.All(ctx) {
		// The error is immediately thrown:
		assert.Error(t, err)
	}
}
