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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/spark-connect-go/v35/spark/sql"
)

func connect() (context.Context, sql.SparkSession) {
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote("sc://localhost").Build(ctx)
	if err != nil {
		panic(err)
	}
	return ctx, spark
}

func createArrowTable() arrow.Table {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "f1-i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "f2-f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "f3-string", Type: &arrow.StringType{}},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec1 := b.NewRecord()
	// Do not release the table
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec1})
	return tbl
}
