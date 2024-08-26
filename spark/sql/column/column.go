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

import proto "github.com/apache/spark-connect-go/v35/internal/generated"

type Column interface {
	ToPlan() (*proto.Expression, error)
	Expr() Expression
	Lt(other Column) Column
	Le(other Column) Column
	Gt(other Column) Column
	Ge(other Column) Column
	Eq(other Column) Column
	Neq(other Column) Column
	Mul(other Column) Column
	Div(other Column) Column
	Alias(alias string) Column
}

type columnImpl struct {
	expr Expression
}

func (c *columnImpl) ToPlan() (*proto.Expression, error) {
	return c.expr.ToPlan()
}

func (c *columnImpl) Expr() Expression {
	return c.expr
}

func (c columnImpl) Lt(other Column) Column {
	return NewColumn(NewUnresolvedFunction("<", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Le(other Column) Column {
	return NewColumn(NewUnresolvedFunction("<=", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Gt(other Column) Column {
	return NewColumn(NewUnresolvedFunction(">", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Ge(other Column) Column {
	return NewColumn(NewUnresolvedFunction(">=", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Eq(other Column) Column {
	return NewColumn(NewUnresolvedFunction("==", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Neq(other Column) Column {
	cmp := NewUnresolvedFunction("==", []Expression{c.expr, other.Expr()}, false)
	return NewColumn(NewUnresolvedFunction("not", []Expression{cmp}, false))
}

func (c columnImpl) Mul(other Column) Column {
	return NewColumn(NewUnresolvedFunction("*", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Div(other Column) Column {
	return NewColumn(NewUnresolvedFunction("/", []Expression{c.expr, other.Expr()}, false))
}

func (c columnImpl) Alias(alias string) Column {
	return NewColumn(NewColumnAlias(alias, c.expr))
}

func NewColumn(expr Expression) Column {
	return &columnImpl{
		expr: expr,
	}
}
