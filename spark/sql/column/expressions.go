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

package column

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/spark-connect-go/v40/spark/sql/types"

	"github.com/apache/spark-connect-go/v40/spark/sparkerrors"

	proto "github.com/apache/spark-connect-go/v40/internal/generated"
)

func newProtoExpression() *proto.Expression {
	return &proto.Expression{}
}

// expression is the interface for all expressions used by Spark Connect.
type expression interface {
	ToProto(context.Context) (*proto.Expression, error)
	DebugString() string
}

type unresolvedRegex struct {
	colRegex string
	planId   *int64
}

func (d *unresolvedRegex) DebugString() string {
	return d.colRegex
}

func (d *unresolvedRegex) ToProto(ctx context.Context) (*proto.Expression, error) {
	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_UnresolvedRegex_{
		UnresolvedRegex: &proto.Expression_UnresolvedRegex{
			ColName: d.colRegex,
			PlanId:  d.planId,
		},
	}
	return expr, nil
}

type delayedColumnReference struct {
	unparsedIdentifier string
	df                 SchemaDataFrame
}

func (d *delayedColumnReference) DebugString() string {
	return d.unparsedIdentifier
}

func (d *delayedColumnReference) ToProto(ctx context.Context) (*proto.Expression, error) {
	// Check if the column identifier is actually part of the schema.
	schema, err := d.df.Schema(ctx)
	if err != nil {
		return nil, err
	}
	found := false
	for _, field := range schema.Fields {
		if field.Name == d.unparsedIdentifier {
			found = true
			break
		}
	}
	// TODO: return proper pyspark error
	if !found {
		return nil, sparkerrors.WithType(sparkerrors.InvalidPlanError,
			fmt.Errorf("cannot resolve column %s", d.unparsedIdentifier))
	}

	expr := newProtoExpression()
	id := d.df.PlanId()
	expr.ExprType = &proto.Expression_UnresolvedAttribute_{
		UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
			UnparsedIdentifier: d.unparsedIdentifier,
			PlanId:             &id,
		},
	}
	return expr, nil
}

type sortExpression struct {
	child        expression
	direction    proto.Expression_SortOrder_SortDirection
	nullOrdering proto.Expression_SortOrder_NullOrdering
}

func (s *sortExpression) DebugString() string {
	return s.child.DebugString()
}

func (s *sortExpression) ToProto(ctx context.Context) (*proto.Expression, error) {
	exp := newProtoExpression()
	child, err := s.child.ToProto(ctx)
	if err != nil {
		return nil, err
	}
	exp.ExprType = &proto.Expression_SortOrder_{
		SortOrder: &proto.Expression_SortOrder{
			Child:        child,
			Direction:    s.direction,
			NullOrdering: s.nullOrdering,
		},
	}
	return exp, nil
}

type caseWhenExpression struct {
	branches []*caseWhenBranch
	elseExpr expression
}

type caseWhenBranch struct {
	condition expression
	value     expression
}

func NewCaseWhenExpression(branches []*caseWhenBranch, elseExpr expression) expression {
	return &caseWhenExpression{branches: branches, elseExpr: elseExpr}
}

func (c *caseWhenExpression) DebugString() string {
	branches := make([]string, 0)
	for _, branch := range c.branches {
		branches = append(branches, fmt.Sprintf("WHEN %s THEN %s",
			branch.condition.DebugString(), branch.value.DebugString()))
	}

	elseExpr := ""
	if c.elseExpr != nil {
		elseExpr = fmt.Sprintf("ELSE %s", c.elseExpr.DebugString())
	}

	return fmt.Sprintf("CASE %s %s END", strings.Join(branches, " "), elseExpr)
}

func (c *caseWhenExpression) ToProto(ctx context.Context) (*proto.Expression, error) {
	args := make([]expression, 0)
	for _, branch := range c.branches {
		args = append(args, branch.condition)
		args = append(args, branch.value)
	}

	if c.elseExpr != nil {
		args = append(args, c.elseExpr)
	}

	fun := NewUnresolvedFunction("when", args, false)
	return fun.ToProto(ctx)
}

type unresolvedExtractValue struct {
	name       string
	child      expression
	extraction expression
}

func (u *unresolvedExtractValue) DebugString() string {
	return fmt.Sprintf("%s(%s, %s)", u.name, u.child.DebugString(), u.extraction.DebugString())
}

func (u *unresolvedExtractValue) ToProto(ctx context.Context) (*proto.Expression, error) {
	expr := newProtoExpression()
	child, err := u.child.ToProto(ctx)
	if err != nil {
		return nil, err
	}

	extraction, err := u.extraction.ToProto(ctx)
	if err != nil {
		return nil, err
	}

	expr.ExprType = &proto.Expression_UnresolvedExtractValue_{
		UnresolvedExtractValue: &proto.Expression_UnresolvedExtractValue{
			Child:      child,
			Extraction: extraction,
		},
	}
	return expr, nil
}

type unresolvedFunction struct {
	name       string
	args       []expression
	isDistinct bool
}

func (u *unresolvedFunction) DebugString() string {
	args := make([]string, 0)
	for _, arg := range u.args {
		args = append(args, arg.DebugString())
	}

	distinct := ""
	if u.isDistinct {
		distinct = "DISTINCT "
	}

	return fmt.Sprintf("%s(%s%s)", u.name, distinct, strings.Join(args, ", "))
}

func (u *unresolvedFunction) ToProto(ctx context.Context) (*proto.Expression, error) {
	// Convert input args to the proto expression.
	var args []*proto.Expression = nil
	if len(u.args) > 0 {
		args = make([]*proto.Expression, 0)
		for _, arg := range u.args {
			p, e := arg.ToProto(ctx)
			if e != nil {
				return nil, e
			}
			args = append(args, p)
		}
	}

	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_UnresolvedFunction_{
		UnresolvedFunction: &proto.Expression_UnresolvedFunction{
			FunctionName: u.name,
			Arguments:    args,
			IsDistinct:   u.isDistinct,
		},
	}
	return expr, nil
}

func NewUnresolvedExtractValue(name string, child expression, extraction expression) expression {
	return &unresolvedExtractValue{name: name, child: child, extraction: extraction}
}

func NewUnresolvedFunction(name string, args []expression, isDistinct bool) expression {
	return &unresolvedFunction{name: name, args: args, isDistinct: isDistinct}
}

func NewUnresolvedFunctionWithColumns(name string, cols ...Column) expression {
	exprs := make([]expression, 0)
	for _, col := range cols {
		exprs = append(exprs, col.expr)
	}
	return NewUnresolvedFunction(name, exprs, false)
}

type columnAlias struct {
	alias    []string
	expr     expression
	metadata *string
}

func NewColumnAlias(alias string, expr expression) expression {
	return &columnAlias{alias: []string{alias}, expr: expr}
}

func NewColumnAliasFromNameParts(alias []string, expr expression) expression {
	return &columnAlias{alias: alias, expr: expr}
}

func (c *columnAlias) DebugString() string {
	child := c.expr.DebugString()
	alias := strings.Join(c.alias, ".")
	return fmt.Sprintf("%s AS %s", child, alias)
}

func (c *columnAlias) ToProto(ctx context.Context) (*proto.Expression, error) {
	expr := newProtoExpression()
	alias, err := c.expr.ToProto(ctx)
	if err != nil {
		return nil, err
	}
	expr.ExprType = &proto.Expression_Alias_{
		Alias: &proto.Expression_Alias{
			Expr:     alias,
			Name:     c.alias,
			Metadata: c.metadata,
		},
	}
	return expr, nil
}

type columnReference struct {
	unparsedIdentifier string
	planId             *int64
}

func NewColumnReference(unparsedIdentifier string) expression {
	return &columnReference{unparsedIdentifier: unparsedIdentifier}
}

func NewColumnReferenceWithPlanId(unparsedIdentifier string, planId int64) expression {
	return &columnReference{unparsedIdentifier: unparsedIdentifier, planId: &planId}
}

func (c *columnReference) DebugString() string {
	return c.unparsedIdentifier
}

func (c *columnReference) ToProto(context.Context) (*proto.Expression, error) {
	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_UnresolvedAttribute_{
		UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
			UnparsedIdentifier: c.unparsedIdentifier,
			PlanId:             c.planId,
		},
	}
	return expr, nil
}

type sqlExression struct {
	expression_string string
}

func NewSQLExpression(expression string) expression {
	return &sqlExression{expression_string: expression}
}

func (s *sqlExression) DebugString() string {
	return s.expression_string
}

func (s *sqlExression) ToProto(context.Context) (*proto.Expression, error) {
	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_ExpressionString_{
		ExpressionString: &proto.Expression_ExpressionString{
			Expression: s.expression_string,
		},
	}
	return expr, nil
}

type literalExpression struct {
	value types.LiteralType
}

func (l *literalExpression) DebugString() string {
	return fmt.Sprintf("%v", l.value)
}

func (l *literalExpression) ToProto(ctx context.Context) (*proto.Expression, error) {
	return l.value.ToProto(ctx)
}

func NewLiteral(value types.LiteralType) expression {
	return &literalExpression{value: value}
}
