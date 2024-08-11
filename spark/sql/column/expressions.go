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
	"fmt"
	"strings"

	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

func newProtoExpression() *proto.Expression {
	return &proto.Expression{}
}

// Expression is the interface for all expressions used by Spark Connect.
type Expression interface {
	ToPlan() (*proto.Expression, error)
	DebugString() string
}

type caseWhenExpression struct {
	branches []*caseWhenBranch
	elseExpr Expression
}

type caseWhenBranch struct {
	condition Expression
	value     Expression
}

func NewCaseWhenExpression(branches []*caseWhenBranch, elseExpr Expression) Expression {
	return &caseWhenExpression{branches: branches, elseExpr: elseExpr}
}

func (c *caseWhenExpression) DebugString() string {
	branches := make([]string, 0)
	for _, branch := range c.branches {
		branches = append(branches, fmt.Sprintf("WHEN %s THEN %s", branch.condition.DebugString(), branch.value.DebugString()))
	}

	elseExpr := ""
	if c.elseExpr != nil {
		elseExpr = fmt.Sprintf("ELSE %s", c.elseExpr.DebugString())
	}

	return fmt.Sprintf("CASE %s %s END", strings.Join(branches, " "), elseExpr)
}

func (c *caseWhenExpression) ToPlan() (*proto.Expression, error) {
	args := make([]Expression, 0)
	for _, branch := range c.branches {
		args = append(args, branch.condition)
		args = append(args, branch.value)
	}

	if c.elseExpr != nil {
		args = append(args, c.elseExpr)
	}

	fun := NewUnresolvedFunction("when", args, false)
	return fun.ToPlan()
}

type unresolvedFunction struct {
	name       string
	args       []Expression
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

func (u *unresolvedFunction) ToPlan() (*proto.Expression, error) {
	// Convert input args to the proto Expression.
	var args []*proto.Expression = nil
	if len(u.args) > 0 {
		args = make([]*proto.Expression, 0)
		for _, arg := range u.args {
			p, e := arg.ToPlan()
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
		},
	}
	return expr, nil
}

func NewUnresolvedFunction(name string, args []Expression, isDistinct bool) Expression {
	return &unresolvedFunction{name: name, args: args, isDistinct: isDistinct}
}

type columnAlias struct {
	alias    []string
	expr     Expression
	metadata *string
}

func NewColumnAlias(alias string, expr Expression) Expression {
	return &columnAlias{alias: []string{alias}, expr: expr}
}

func NewColumnAliasFromNameParts(alias []string, expr Expression) Expression {
	return &columnAlias{alias: alias, expr: expr}
}

func (c *columnAlias) DebugString() string {
	child := c.expr.DebugString()
	alias := strings.Join(c.alias, ".")
	return fmt.Sprintf("%s AS %s", child, alias)
}

func (c *columnAlias) ToPlan() (*proto.Expression, error) {
	expr := newProtoExpression()
	alias, err := c.expr.ToPlan()
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

func NewColumnReference(unparsedIdentifier string) Expression {
	return &columnReference{unparsedIdentifier: unparsedIdentifier}
}

func NewColumnReferenceWithPlanId(unparsedIdentifier string, planId int64) Expression {
	return &columnReference{unparsedIdentifier: unparsedIdentifier, planId: &planId}
}

func (c *columnReference) DebugString() string {
	return c.unparsedIdentifier
}

func (c *columnReference) ToPlan() (*proto.Expression, error) {
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

func NewSQLExpression(expression string) Expression {
	return &sqlExression{expression_string: expression}
}

func (s *sqlExression) DebugString() string {
	return s.expression_string
}

func (s *sqlExression) ToPlan() (*proto.Expression, error) {
	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_ExpressionString_{
		ExpressionString: &proto.Expression_ExpressionString{
			Expression: s.expression_string,
		},
	}
	return expr, nil
}

type literalExpression struct {
	value any
}

func (l *literalExpression) DebugString() string {
	return fmt.Sprintf("%v", l.value)
}

func (l *literalExpression) ToPlan() (*proto.Expression, error) {
	expr := newProtoExpression()
	expr.ExprType = &proto.Expression_Literal_{
		Literal: &proto.Expression_Literal{},
	}
	switch v := l.value.(type) {
	case int8:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Byte{Byte: int32(v)}
	case int16:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Short{Short: int32(v)}
	case int32:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Integer{Integer: v}
	case int64:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Long{Long: v}
	case uint8:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Short{Short: int32(v)}
	case uint16:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Integer{Integer: int32(v)}
	case uint32:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Long{Long: int64(v)}
	case float32:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Float{Float: v}
	case float64:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Double{Double: v}
	case string:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_String_{String_: v}
	case bool:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Boolean{Boolean: v}
	case []byte:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Binary{Binary: v}
	case int:
		expr.GetLiteral().LiteralType = &proto.Expression_Literal_Long{Long: int64(v)}
	default:
		return nil, sparkerrors.WithType(sparkerrors.InvalidPlanError,
			fmt.Errorf("unsupported literal type %T", v))
	}
	return expr, nil
}

func NewLiteral(value any) Expression {
	return &literalExpression{value: value}
}
