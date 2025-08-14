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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/spark-connect-go/v40/spark/sql/types"
	"github.com/stretchr/testify/assert"
)

func TestSparkSession_CreateDataFrame_StructTypeToArrowConversion(t *testing.T) {
	// This test validates that the fix for StructType.ToArrowType() works correctly
	// The change from .Fields() to .(*arrow.StructType).Fields() should be validated

	// Create a test schema
	schema := types.StructOf(
		types.NewStructField("id", types.INTEGER),
		types.NewStructField("name", types.STRING),
		types.NewStructField("scores", types.ArrayType{
			ElementType:  types.DOUBLE,
			ContainsNull: true,
		}),
	)

	// Test that ToArrowType returns the correct interface type
	arrowType := schema.ToArrowType()
	assert.NotNil(t, arrowType)

	// Verify we can cast it to *arrow.StructType and access Fields()
	structType, ok := arrowType.(*arrow.StructType)
	assert.True(t, ok)

	fields := structType.Fields()
	assert.Len(t, fields, 3)
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, "scores", fields[2].Name)

	// Test sample data that would work with CreateDataFrame
	data := [][]any{
		{1, "Alice", []float64{95.5, 87.2, 92.1}},
		{2, "Bob", []float64{88.0, 91.5, 89.3}},
	}

	// Verify that the data structure is compatible with the schema
	assert.Len(t, data, 2)
	for _, row := range data {
		assert.Len(t, row, 3)
		assert.IsType(t, 1, row[0])           // integer id
		assert.IsType(t, "", row[1])          // string name
		assert.IsType(t, []float64{}, row[2]) // array of doubles
	}

	// Test that we can create an Arrow schema using the converted type
	pool := memory.NewGoAllocator()
	_ = pool // Use the pool variable to avoid unused error

	// This would previously fail due to the type assertion issue
	// Now it should work because ToArrowType() returns arrow.DataType interface
	arrowSchema := arrow.NewSchema(structType.Fields(), nil)
	assert.NotNil(t, arrowSchema)
	assert.Equal(t, 3, len(arrowSchema.Fields()))
}
