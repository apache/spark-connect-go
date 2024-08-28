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

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/apache/spark-connect-go/v35/spark/sql/column"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"
)

type GroupedData struct {
	df           dataFrameImpl
	groupType    string
	groupingCols []column.Convertible
	pivotValues  []any
	// groupingSets [][]column.Column
}

// Agg compute aggregates and returns the result as a DataFrame. The aggegrate expressions
// are passed as column.Column arguments.
func (gd *GroupedData) Agg(exprs ...column.Column) (DataFrame, error) {
	if len(exprs) == 0 {
		return nil, sparkerrors.WithString(sparkerrors.InvalidInputError, "exprs should not be empty")
	}

	agg := &proto.Aggregate{
		Input: gd.df.relation,
	}

	// Add all grouping and aggregate expressions.
	agg.GroupingExpressions = make([]*proto.Expression, len(gd.groupingCols))
	for i, col := range gd.groupingCols {
		exp, err := col.ToPlan()
		if err != nil {
			return nil, err
		}
		agg.GroupingExpressions[i] = exp
	}

	agg.AggregateExpressions = make([]*proto.Expression, len(exprs))
	for i, expr := range exprs {
		exp, err := expr.ToPlan()
		if err != nil {
			return nil, err
		}
		agg.AggregateExpressions[i] = exp
	}

	// Apply the groupType
	switch gd.groupType {
	case "pivot":
		agg.GroupType = proto.Aggregate_GROUP_TYPE_PIVOT
		// Apply all pivot behavior and convert columns into literals.
		if len(gd.pivotValues) == 0 {
			return nil, sparkerrors.WithString(sparkerrors.InvalidInputError, "pivotValues should not be empty")
		}
		agg.Pivot = &proto.Aggregate_Pivot{
			Values: make([]*proto.Expression_Literal, len(gd.pivotValues)),
		}
		for i, v := range gd.pivotValues {
			exp, err := column.NewLiteral(v).ToPlan()
			if err != nil {
				return nil, err
			}
			agg.Pivot.Values[i] = exp.GetLiteral()
		}
	case "groupby":
		agg.GroupType = proto.Aggregate_GROUP_TYPE_GROUPBY
	case "rollup":
		agg.GroupType = proto.Aggregate_GROUP_TYPE_ROLLUP
	case "cube":
		agg.GroupType = proto.Aggregate_GROUP_TYPE_CUBE
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Aggregate{
			Aggregate: agg,
		},
	}
	return NewDataFrame(gd.df.session, rel), nil
}

func (gd *GroupedData) numericAgg(name string, cols ...string) (DataFrame, error) {
	// TODO: Fix getting the context here...
	ctx := context.Background()
	schema, err := gd.df.Schema(ctx)
	if err != nil {
		return nil, err
	}

	// Find all numeric cols in the schema:
	numericCols := make([]string, 0)
	for _, field := range schema.Fields {
		if field.DataType.IsNumeric() {
			numericCols = append(numericCols, field.Name)
		}
	}

	aggCols := cols
	if len(cols) > 0 {
		invalidCols := make([]string, 0)
		for _, col := range cols {
			found := false
			for _, nc := range numericCols {
				if col == nc {
					found = true
				}
			}
			if !found {
				invalidCols = append(invalidCols, col)
			}
		}
		if len(invalidCols) > 0 {
			return nil, sparkerrors.WithStringf(sparkerrors.InvalidInputError,
				"columns %v are not numeric", invalidCols)
		}
	} else {
		aggCols = numericCols
	}

	finalColumns := make([]column.Column, len(aggCols))
	for i, col := range aggCols {
		finalColumns[i] = column.NewColumn(column.NewUnresolvedFunction(name, []column.Expression{
			functions.Col(col).Expr(),
		}, false))
	}
	return gd.Agg(finalColumns...)
}

// Min Computes the min value for each numeric column for each group.
func (gd *GroupedData) Min(cols ...string) (DataFrame, error) {
	return gd.numericAgg("min", cols...)
}

// Max Computes the max value for each numeric column for each group.
func (gd *GroupedData) Max(cols ...string) (DataFrame, error) {
	return gd.numericAgg("max", cols...)
}

// Avg Computes the avg value for each numeric column for each group.
func (gd *GroupedData) Avg(cols ...string) (DataFrame, error) {
	return gd.numericAgg("avg", cols...)
}

// Sum Computes the sum value for each numeric column for each group.
func (gd *GroupedData) Sum(cols ...string) (DataFrame, error) {
	return gd.numericAgg("sum", cols...)
}

// Count Computes the count value for each group.
func (gd *GroupedData) Count() (DataFrame, error) {
	return gd.Agg(functions.Count(functions.Lit(1)).Alias("count"))
}
