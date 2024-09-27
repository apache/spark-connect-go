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
	df           *dataFrameImpl
	groupType    string
	groupingCols []column.Convertible
	pivotValues  []any
	pivotCol     column.Convertible
}

// Agg compute aggregates and returns the result as a DataFrame. The aggegrate expressions
// are passed as column.Column arguments.
func (gd *GroupedData) Agg(ctx context.Context, exprs ...column.Column) (DataFrame, error) {
	if len(exprs) == 0 {
		return nil, sparkerrors.WithString(sparkerrors.InvalidInputError, "exprs should not be empty")
	}

	agg := &proto.Aggregate{
		Input: gd.df.relation,
	}

	// Add all grouping and aggregate expressions.
	agg.GroupingExpressions = make([]*proto.Expression, len(gd.groupingCols))
	for i, col := range gd.groupingCols {
		exp, err := col.ToProto(ctx)
		if err != nil {
			return nil, err
		}
		agg.GroupingExpressions[i] = exp
	}

	agg.AggregateExpressions = make([]*proto.Expression, len(exprs))
	for i, expr := range exprs {
		exp, err := expr.ToProto(ctx)
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
		protoCol, err := gd.pivotCol.ToProto(ctx)
		if err != nil {
			return nil, err
		}

		agg.Pivot = &proto.Aggregate_Pivot{
			Values: make([]*proto.Expression_Literal, len(gd.pivotValues)),
			Col:    protoCol,
		}
		for i, v := range gd.pivotValues {
			exp, err := column.NewLiteral(v).ToProto(ctx)
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

func (gd *GroupedData) numericAgg(ctx context.Context, name string, cols ...string) (DataFrame, error) {
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
		finalColumns[i] = column.NewColumn(column.NewUnresolvedFunctionWithColumns(name, functions.Col(col)))
	}
	return gd.Agg(ctx, finalColumns...)
}

// Min Computes the min value for each numeric column for each group.
func (gd *GroupedData) Min(ctx context.Context, cols ...string) (DataFrame, error) {
	return gd.numericAgg(ctx, "min", cols...)
}

// Max Computes the max value for each numeric column for each group.
func (gd *GroupedData) Max(ctx context.Context, cols ...string) (DataFrame, error) {
	return gd.numericAgg(ctx, "max", cols...)
}

// Avg Computes the avg value for each numeric column for each group.
func (gd *GroupedData) Avg(ctx context.Context, cols ...string) (DataFrame, error) {
	return gd.numericAgg(ctx, "avg", cols...)
}

// Sum Computes the sum value for each numeric column for each group.
func (gd *GroupedData) Sum(ctx context.Context, cols ...string) (DataFrame, error) {
	return gd.numericAgg(ctx, "sum", cols...)
}

// Count Computes the count value for each group.
func (gd *GroupedData) Count(ctx context.Context) (DataFrame, error) {
	return gd.Agg(ctx, functions.Count(functions.Lit(1)).Alias("count"))
}

// Mean Computes the average value for each numeric column for each group.
func (gd *GroupedData) Mean(ctx context.Context, cols ...string) (DataFrame, error) {
	return gd.Avg(ctx, cols...)
}

func (gd *GroupedData) Pivot(ctx context.Context, pivotCol string, pivotValues []any) (*GroupedData, error) {
	if gd.groupType != "groupby" {
		if gd.groupType == "pivot" {
			return nil, sparkerrors.WithString(sparkerrors.InvalidInputError, "pivot cannot be applied on pivot")
		}
		return nil, sparkerrors.WithString(sparkerrors.InvalidInputError, "pivot can only be applied on groupby")
	}
	return &GroupedData{
		df:           gd.df,
		groupType:    "pivot",
		groupingCols: gd.groupingCols,
		pivotValues:  pivotValues,
		pivotCol:     column.NewColumnReferenceWithPlanId(pivotCol, gd.df.PlanId()),
	}, nil
}
