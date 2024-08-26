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
	"fmt"

	"github.com/apache/spark-connect-go/v35/spark/sql/column"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"

	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

// ResultCollector receives a stream of result rows
type ResultCollector interface {
	// WriteRow receives a single row from the data frame
	WriteRow(values []any)
}

// DataFrame is a wrapper for data frame, representing a distributed collection of data row.
type DataFrame interface {
	// WriteResult streams the data frames to a result collector
	WriteResult(ctx context.Context, collector ResultCollector, numRows int, truncate bool) error
	// Show uses WriteResult to write the data frames to the console output.
	Show(ctx context.Context, numRows int, truncate bool) error
	// Schema returns the schema for the current data frame.
	Schema(ctx context.Context) (*types.StructType, error)
	// Collect returns the data rows of the current data frame.
	Collect(ctx context.Context) ([]Row, error)
	// Writer returns a data frame writer, which could be used to save data frame to supported storage.
	Writer() DataFrameWriter
	// Write is an alias for Writer
	// Deprecated: Use Writer
	Write() DataFrameWriter
	// CreateTempView creates or replaces a temporary view.
	CreateTempView(ctx context.Context, viewName string, replace, global bool) error
	// Repartition re-partitions a data frame.
	Repartition(numPartitions int, columns []string) (DataFrame, error)
	// RepartitionByRange re-partitions a data frame by range partition.
	RepartitionByRange(numPartitions int, columns []RangePartitionColumn) (DataFrame, error)
	// Filter filters the data frame by a column condition.
	Filter(condition column.Column) (DataFrame, error)
	// FilterByString filters the data frame by a string condition.
	FilterByString(condition string) (DataFrame, error)
	// Col returns a column by name.
	Col(name string) (column.Column, error)

	// Select projects a list of columns from the DataFrame
	Select(columns ...column.Column) (DataFrame, error)
	// SelectExpr projects a list of columns from the DataFrame by string expressions
	SelectExpr(exprs ...string) (DataFrame, error)
	// Alias creates a new DataFrame with the specified subquery alias
	Alias(alias string) DataFrame
	// CrossJoin joins the current DataFrame with another DataFrame using the cross product
	CrossJoin(other DataFrame) DataFrame
}

type RangePartitionColumn struct {
	Name       string
	Descending bool
}

// dataFrameImpl is an implementation of DataFrame interface.
type dataFrameImpl struct {
	session  *sparkSessionImpl
	relation *proto.Relation // TODO change to proto.Plan?
}

func (df *dataFrameImpl) SelectExpr(exprs ...string) (DataFrame, error) {
	expressions := make([]*proto.Expression, 0, len(exprs))
	for _, expr := range exprs {
		col := functions.Expr(expr)
		f, e := col.ToPlan()
		if e != nil {
			return nil, e
		}
		expressions = append(expressions, f)
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Project{
			Project: &proto.Project{
				Input:       df.relation,
				Expressions: expressions,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) Alias(alias string) DataFrame {
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_SubqueryAlias{
			SubqueryAlias: &proto.SubqueryAlias{
				Input: df.relation,
				Alias: alias,
			},
		},
	}
	return NewDataFrame(df.session, rel)
}

func (df *dataFrameImpl) CrossJoin(other DataFrame) DataFrame {
	otherDf := other.(*dataFrameImpl)
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Join{
			Join: &proto.Join{
				Left:     df.relation,
				Right:    otherDf.relation,
				JoinType: proto.Join_JOIN_TYPE_CROSS,
			},
		},
	}
	return NewDataFrame(df.session, rel)
}

// NewDataFrame creates a new DataFrame
func NewDataFrame(session *sparkSessionImpl, relation *proto.Relation) DataFrame {
	return &dataFrameImpl{
		session:  session,
		relation: relation,
	}
}

type consoleCollector struct{}

func (c consoleCollector) WriteRow(values []any) {
	fmt.Println(values...)
}

func (df *dataFrameImpl) Show(ctx context.Context, numRows int, truncate bool) error {
	return df.WriteResult(ctx, &consoleCollector{}, numRows, truncate)
}

func (df *dataFrameImpl) WriteResult(ctx context.Context, collector ResultCollector, numRows int, truncate bool) error {
	truncateValue := 0
	if truncate {
		truncateValue = 20
	}
	vertical := false

	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: &proto.Relation_ShowString{
					ShowString: &proto.ShowString{
						Input:    df.relation,
						NumRows:  int32(numRows),
						Truncate: int32(truncateValue),
						Vertical: vertical,
					},
				},
			},
		},
	}

	responseClient, err := df.session.client.ExecutePlan(ctx, plan)
	if err != nil {
		return sparkerrors.WithType(fmt.Errorf("failed to show dataframe: %w", err), sparkerrors.ExecutionError)
	}

	schema, table, err := responseClient.ToTable()
	if err != nil {
		return err
	}

	rows := make([]Row, table.NumRows())

	values, err := types.ReadArrowTable(table)
	if err != nil {
		return err
	}

	for idx, v := range values {
		row := NewRowWithSchema(v, schema)
		rows[idx] = row
	}

	for _, row := range rows {
		values, err := row.Values()
		if err != nil {
			return sparkerrors.WithType(fmt.Errorf(
				"failed to get values in the row: %w", err), sparkerrors.ReadError)
		}
		collector.WriteRow(values)
	}
	return nil
}

func (df *dataFrameImpl) Schema(ctx context.Context) (*types.StructType, error) {
	response, err := df.session.client.AnalyzePlan(ctx, df.createPlan())
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to analyze plan: %w", err), sparkerrors.ExecutionError)
	}

	responseSchema := response.GetSchema().Schema
	return types.ConvertProtoDataTypeToStructType(responseSchema)
}

func (df *dataFrameImpl) Collect(ctx context.Context) ([]Row, error) {
	responseClient, err := df.session.client.ExecutePlan(ctx, df.createPlan())
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to execute plan: %w", err), sparkerrors.ExecutionError)
	}

	var schema *types.StructType
	schema, table, err := responseClient.ToTable()
	if err != nil {
		return nil, err
	}

	rows := make([]Row, table.NumRows())

	values, err := types.ReadArrowTable(table)
	if err != nil {
		return nil, err
	}

	for idx, v := range values {
		row := NewRowWithSchema(v, schema)
		rows[idx] = row
	}
	return rows, nil
}

func (df *dataFrameImpl) Write() DataFrameWriter {
	return df.Writer()
}

func (df *dataFrameImpl) Writer() DataFrameWriter {
	return newDataFrameWriter(df.session, df.relation)
}

func (df *dataFrameImpl) CreateTempView(ctx context.Context, viewName string, replace, global bool) error {
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_CreateDataframeView{
					CreateDataframeView: &proto.CreateDataFrameViewCommand{
						Input:    df.relation,
						Name:     viewName,
						Replace:  replace,
						IsGlobal: global,
					},
				},
			},
		},
	}

	responseClient, err := df.session.client.ExecutePlan(ctx, plan)
	if err != nil {
		return sparkerrors.WithType(fmt.Errorf("failed to create temp view %s: %w",
			viewName, err), sparkerrors.ExecutionError)
	}

	_, _, err = responseClient.ToTable()
	return err
}

func (df *dataFrameImpl) Repartition(numPartitions int, columns []string) (DataFrame, error) {
	var partitionExpressions []*proto.Expression
	if columns != nil {
		partitionExpressions = make([]*proto.Expression, 0, len(columns))
		for _, c := range columns {
			expr := &proto.Expression{
				ExprType: &proto.Expression_UnresolvedAttribute_{
					UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
						UnparsedIdentifier: c,
					},
				},
			}
			partitionExpressions = append(partitionExpressions, expr)
		}
	}
	return df.repartitionByExpressions(numPartitions, partitionExpressions)
}

func (df *dataFrameImpl) RepartitionByRange(numPartitions int, columns []RangePartitionColumn) (DataFrame, error) {
	var partitionExpressions []*proto.Expression
	if columns != nil {
		partitionExpressions = make([]*proto.Expression, 0, len(columns))
		for _, c := range columns {
			columnExpr := &proto.Expression{
				ExprType: &proto.Expression_UnresolvedAttribute_{
					UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
						UnparsedIdentifier: c.Name,
					},
				},
			}
			direction := proto.Expression_SortOrder_SORT_DIRECTION_ASCENDING
			if c.Descending {
				direction = proto.Expression_SortOrder_SORT_DIRECTION_DESCENDING
			}
			sortExpr := &proto.Expression{
				ExprType: &proto.Expression_SortOrder_{
					SortOrder: &proto.Expression_SortOrder{
						Child:     columnExpr,
						Direction: direction,
					},
				},
			}
			partitionExpressions = append(partitionExpressions, sortExpr)
		}
	}
	return df.repartitionByExpressions(numPartitions, partitionExpressions)
}

func (df *dataFrameImpl) createPlan() *proto.Plan {
	return &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: df.relation,
		},
	}
}

func (df *dataFrameImpl) repartitionByExpressions(numPartitions int,
	partitionExpressions []*proto.Expression,
) (DataFrame, error) {
	var numPartitionsPointerValue *int32
	if numPartitions != 0 {
		int32Value := int32(numPartitions)
		numPartitionsPointerValue = &int32Value
	}
	df.relation.GetRepartitionByExpression()
	newRelation := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_RepartitionByExpression{
			RepartitionByExpression: &proto.RepartitionByExpression{
				Input:          df.relation,
				NumPartitions:  numPartitionsPointerValue,
				PartitionExprs: partitionExpressions,
			},
		},
	}
	return NewDataFrame(df.session, newRelation), nil
}

func (df *dataFrameImpl) Filter(condition column.Column) (DataFrame, error) {
	cnd, err := condition.ToPlan()
	if err != nil {
		return nil, err
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Filter{
			Filter: &proto.Filter{
				Input:     df.relation,
				Condition: cnd,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) FilterByString(condition string) (DataFrame, error) {
	return df.Filter(functions.Expr(condition))
}

func (df *dataFrameImpl) Col(name string) (column.Column, error) {
	planId := df.relation.Common.GetPlanId()
	return column.NewColumn(column.NewColumnReferenceWithPlanId(name, planId)), nil
}

func (df *dataFrameImpl) Select(columns ...column.Column) (DataFrame, error) {
	exprs := make([]*proto.Expression, 0, len(columns))
	for _, c := range columns {
		expr, err := c.ToPlan()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Project{
			Project: &proto.Project{
				Input:       df.relation,
				Expressions: exprs,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}
