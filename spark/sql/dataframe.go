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
	// PlanId returns the plan id of the data frame.
	PlanId() int64
	// Alias creates a new DataFrame with the specified subquery alias
	Alias(ctx context.Context, alias string) DataFrame
	// Coalesce returns a new DataFrame that has exactly numPartitions partitions.DataFrame
	//
	// Similar to coalesce defined on an :class:`RDD`, this operation results in a
	// narrow dependency, e.g. if you go from 1000 partitions to 100 partitions,
	// there will not be a shuffle, instead each of the 100 new partitions will
	// claim 10 of the current partitions. If a larger number of partitions is requested,
	// it will stay at the current number of partitions.
	//
	// However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
	// this may result in your computation taking place on fewer nodes than
	// you like (e.g. one node in the case of numPartitions = 1). To avoid this,
	// you can call repartition(). This will add a shuffle step, but means the
	// current upstream partitions will be executed in parallel (per whatever
	// the current partitioning is).
	Coalesce(ctx context.Context, numPartitions int) DataFrame
	// Columns returns the list of column names of the DataFrame.
	Columns(ctx context.Context) ([]string, error)
	// Corr calculates the correlation of two columns of a :class:`DataFrame` as a double value.
	// Currently only supports the Pearson Correlation Coefficient.
	Corr(ctx context.Context, col1, col2 string) (float64, error)
	CorrWithMethod(ctx context.Context, col1, col2 string, method string) (float64, error)
	// Count returns the number of rows in the DataFrame.
	Count(ctx context.Context) (int64, error)
	// Cov calculates the sample covariance for the given columns, specified by their names, as a
	// double value.
	Cov(ctx context.Context, col1, col2 string) (float64, error)
	// Collect returns the data rows of the current data frame.
	Collect(ctx context.Context) ([]Row, error)
	// CreateTempView creates or replaces a temporary view.
	CreateTempView(ctx context.Context, viewName string, replace, global bool) error
	// CrossJoin joins the current DataFrame with another DataFrame using the cross product
	CrossJoin(ctx context.Context, other DataFrame) DataFrame
	Drop(ctx context.Context, columns ...column.Convertible) (DataFrame, error)
	DropByName(ctx context.Context, columns ...string) (DataFrame, error)
	DropDuplicates(ctx context.Context, columns ...string) (DataFrame, error)
	// Filter filters the data frame by a column condition.
	Filter(ctx context.Context, condition column.Convertible) (DataFrame, error)
	// FilterByString filters the data frame by a string condition.
	FilterByString(ctx context.Context, condition string) (DataFrame, error)
	// GroupBy groups the DataFrame by the spcified columns so that the aggregation
	// can be performed on them. See GroupedData for all the available aggregate functions.
	GroupBy(cols ...column.Convertible) *GroupedData
	// Repartition re-partitions a data frame.
	Repartition(ctx context.Context, numPartitions int, columns []string) (DataFrame, error)
	// RepartitionByRange re-partitions a data frame by range partition.
	RepartitionByRange(ctx context.Context, numPartitions int, columns ...column.Convertible) (DataFrame, error)
	// Show uses WriteResult to write the data frames to the console output.
	Show(ctx context.Context, numRows int, truncate bool) error
	// Schema returns the schema for the current data frame.
	Schema(ctx context.Context) (*types.StructType, error)
	// Select projects a list of columns from the DataFrame
	Select(ctx context.Context, columns ...column.Convertible) (DataFrame, error)
	// SelectExpr projects a list of columns from the DataFrame by string expressions
	SelectExpr(ctx context.Context, exprs ...string) (DataFrame, error)
	// WithColumn returns a new DataFrame by adding a column or replacing the
	// existing column that has the same name. The column expression must be an
	// expression over this DataFrame; attempting to add a column from some other
	// DataFrame will raise an error.
	//
	// Note: This method introduces a projection internally. Therefore, calling it multiple
	// times, for instance, via loops in order to add multiple columns can generate big
	// plans which can cause performance issues and even `StackOverflowException`.
	// To avoid this, use :func:`select` with multiple columns at once.
	WithColumn(ctx context.Context, colName string, col column.Convertible) (DataFrame, error)
	WithColumns(ctx context.Context, colsMap map[string]column.Convertible) (DataFrame, error)
	// WithColumnRenamed returns a new DataFrame by renaming an existing column.
	// This is a no-op if the schema doesn't contain the given column name.
	WithColumnRenamed(ctx context.Context, existingName, newName string) (DataFrame, error)
	// WithColumnsRenamed returns a new DataFrame by renaming multiple existing columns.
	WithColumnsRenamed(ctx context.Context, colsMap map[string]string) (DataFrame, error)
	// WithMetadata returns a new DataFrame with the specified metadata for each of the columns.
	WithMetadata(ctx context.Context, metadata map[string]string) (DataFrame, error)
	WithWatermark(ctx context.Context, eventTime string, delayThreshold string) (DataFrame, error)
	Where(ctx context.Context, condition string) (DataFrame, error)
	// Writer returns a data frame writer, which could be used to save data frame to supported storage.
	Writer() DataFrameWriter
	// Write is an alias for Writer
	// Deprecated: Use Writer
	Write() DataFrameWriter
	// WriteResult streams the data frames to a result collector
	WriteResult(ctx context.Context, collector ResultCollector, numRows int, truncate bool) error
}

// dataFrameImpl is an implementation of DataFrame interface.
type dataFrameImpl struct {
	session  *sparkSessionImpl
	relation *proto.Relation // TODO change to proto.Plan?
}

func (df *dataFrameImpl) Coalesce(ctx context.Context, numPartitions int) DataFrame {
	shuffle := false
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Repartition{
			Repartition: &proto.Repartition{
				Input:         df.relation,
				Shuffle:       &shuffle,
				NumPartitions: int32(numPartitions),
			},
		},
	}
	return NewDataFrame(df.session, rel)
}

func (df *dataFrameImpl) Columns(ctx context.Context) ([]string, error) {
	schema, err := df.Schema(ctx)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		columns[i] = field.Name
	}
	return columns, nil
}

func (df *dataFrameImpl) Corr(ctx context.Context, col1, col2 string) (float64, error) {
	return df.CorrWithMethod(ctx, col1, col2, "pearson")
}

func (df *dataFrameImpl) CorrWithMethod(ctx context.Context, col1, col2 string, method string) (float64, error) {
	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: &proto.Relation_Corr{
					Corr: &proto.StatCorr{
						Input:  df.relation,
						Col1:   col1,
						Col2:   col2,
						Method: &method,
					},
				},
			},
		},
	}

	responseClient, err := df.session.client.ExecutePlan(ctx, plan)
	if err != nil {
		return 0, sparkerrors.WithType(fmt.Errorf("failed to execute plan: %w", err), sparkerrors.ExecutionError)
	}

	_, table, err := responseClient.ToTable()
	if err != nil {
		return 0, err
	}

	values, err := types.ReadArrowTable(table)
	if err != nil {
		return 0, err
	}

	return values[0][0].(float64), nil
}

func (df *dataFrameImpl) Count(ctx context.Context) (int64, error) {
	res, err := df.GroupBy().Count(ctx)
	if err != nil {
		return 0, err
	}
	rows, err := res.Collect(ctx)
	if err != nil {
		return 0, err
	}
	row, err := rows[0].Values()
	if err != nil {
		return 0, err
	}
	return row[0].(int64), nil
}

func (df *dataFrameImpl) Cov(ctx context.Context, col1, col2 string) (float64, error) {
	plan := &proto.Plan{
		OpType: &proto.Plan_Root{
			Root: &proto.Relation{
				Common: &proto.RelationCommon{
					PlanId: newPlanId(),
				},
				RelType: &proto.Relation_Cov{
					Cov: &proto.StatCov{
						Input: df.relation,
						Col1:  col1,
						Col2:  col2,
					},
				},
			},
		},
	}

	responseClient, err := df.session.client.ExecutePlan(ctx, plan)
	if err != nil {
		return 0, sparkerrors.WithType(fmt.Errorf("failed to execute plan: %w", err), sparkerrors.ExecutionError)
	}

	_, table, err := responseClient.ToTable()
	if err != nil {
		return 0, err
	}

	values, err := types.ReadArrowTable(table)
	if err != nil {
		return 0, err
	}

	return values[0][0].(float64), nil
}

func (df *dataFrameImpl) PlanId() int64 {
	return df.relation.GetCommon().GetPlanId()
}

func (df *dataFrameImpl) SelectExpr(ctx context.Context, exprs ...string) (DataFrame, error) {
	expressions := make([]*proto.Expression, 0, len(exprs))
	for _, expr := range exprs {
		col := column.NewSQLExpression(expr)
		f, e := col.ToProto(ctx)
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

func (df *dataFrameImpl) Alias(ctx context.Context, alias string) DataFrame {
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

func (df *dataFrameImpl) CrossJoin(ctx context.Context, other DataFrame) DataFrame {
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

func (df *dataFrameImpl) Repartition(ctx context.Context, numPartitions int, columns []string) (DataFrame, error) {
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

func (df *dataFrameImpl) RepartitionByRange(ctx context.Context, numPartitions int, columns ...column.Convertible) (DataFrame, error) {
	var partitionExpressions []*proto.Expression
	if columns != nil {
		partitionExpressions = make([]*proto.Expression, 0, len(columns))
		for _, c := range columns {
			expr, err := c.ToProto(ctx)
			if err != nil {
				return nil, err
			}
			partitionExpressions = append(partitionExpressions, expr)
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

func (df *dataFrameImpl) Filter(ctx context.Context, condition column.Convertible) (DataFrame, error) {
	cnd, err := condition.ToProto(ctx)
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

func (df *dataFrameImpl) FilterByString(ctx context.Context, condition string) (DataFrame, error) {
	return df.Filter(ctx, column.NewColumn(column.NewSQLExpression(condition)))
}

func (df *dataFrameImpl) Select(ctx context.Context, columns ...column.Convertible) (DataFrame, error) {
	exprs := make([]*proto.Expression, 0, len(columns))
	for _, c := range columns {
		expr, err := c.ToProto(ctx)
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

// GroupBy groups the DataFrame by the specified columns so that aggregation
// can be performed on them. See GroupedData for all the available aggregate functions.
func (df *dataFrameImpl) GroupBy(cols ...column.Convertible) *GroupedData {
	return &GroupedData{
		df:           *df,
		groupingCols: cols,
		groupType:    "groupby",
	}
}

func (df *dataFrameImpl) WithColumn(ctx context.Context, colName string, col column.Convertible) (DataFrame, error) {
	return df.WithColumns(ctx, map[string]column.Convertible{colName: col})
}

func (df *dataFrameImpl) WithColumns(ctx context.Context, colsMap map[string]column.Convertible) (DataFrame, error) {
	// Convert all columns to proto expressions and the corresponding alias:
	aliases := make([]*proto.Expression_Alias, 0, len(colsMap))
	for colName, col := range colsMap {
		expr, err := col.ToProto(ctx)
		if err != nil {
			return nil, err
		}
		alias := &proto.Expression_Alias{
			Expr: expr,
			Name: []string{colName},
		}
		aliases = append(aliases, alias)
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_WithColumns{
			WithColumns: &proto.WithColumns{
				Input:   df.relation,
				Aliases: aliases,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) WithColumnRenamed(ctx context.Context, existingName, newName string) (DataFrame, error) {
	return df.WithColumnsRenamed(ctx, map[string]string{existingName: newName})
}

func (df *dataFrameImpl) WithColumnsRenamed(ctx context.Context, colsMap map[string]string) (DataFrame, error) {
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_WithColumnsRenamed{
			WithColumnsRenamed: &proto.WithColumnsRenamed{
				Input:            df.relation,
				RenameColumnsMap: colsMap,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) WithMetadata(ctx context.Context, metadata map[string]string) (DataFrame, error) {
	// WithMetadata works the same way as with columns but extracts the column reference from the DataFrame
	// and injects it back into the projection.
	aliases := make([]*proto.Expression_Alias, 0, len(metadata))
	for colName, metadata := range metadata {
		expr := column.OfDF(df, colName)
		exprProto, err := expr.ToProto(ctx)
		if err != nil {
			return nil, err
		}
		alias := &proto.Expression_Alias{
			Expr:     exprProto,
			Name:     []string{colName},
			Metadata: &metadata,
		}
		aliases = append(aliases, alias)
	}
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_WithColumns{
			WithColumns: &proto.WithColumns{
				Input:   df.relation,
				Aliases: aliases,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) WithWatermark(ctx context.Context, eventTime string, delayThreshold string) (DataFrame, error) {
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_WithWatermark{
			WithWatermark: &proto.WithWatermark{
				Input:          df.relation,
				EventTime:      eventTime,
				DelayThreshold: delayThreshold,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) Where(ctx context.Context, condition string) (DataFrame, error) {
	return df.FilterByString(ctx, condition)
}

func (df *dataFrameImpl) Drop(ctx context.Context, columns ...column.Convertible) (DataFrame, error) {
	exprs := make([]*proto.Expression, 0, len(columns))
	for _, c := range columns {
		e, err := c.ToProto(ctx)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}

	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Drop{
			Drop: &proto.Drop{
				Input:   df.relation,
				Columns: exprs,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) DropByName(ctx context.Context, columns ...string) (DataFrame, error) {
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Drop{
			Drop: &proto.Drop{
				Input:       df.relation,
				ColumnNames: columns,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}

func (df *dataFrameImpl) DropDuplicates(ctx context.Context, columns ...string) (DataFrame, error) {
	withinWatermark := false
	allColumnsAsKeys := len(columns) == 0
	rel := &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Deduplicate{
			Deduplicate: &proto.Deduplicate{
				Input:            df.relation,
				ColumnNames:      columns,
				WithinWatermark:  &withinWatermark,
				AllColumnsAsKeys: &allColumnsAsKeys,
			},
		},
	}
	return NewDataFrame(df.session, rel), nil
}
