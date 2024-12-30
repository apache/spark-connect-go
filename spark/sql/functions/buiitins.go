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

package functions

import (
	"github.com/apache/spark-connect-go/v35/spark/sql/column"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

func Expr(expr string) column.Column {
	return column.NewColumn(column.NewSQLExpression(expr))
}

func Col(name string) column.Column {
	return column.NewColumn(column.NewColumnReference(name))
}

func Lit(value types.LiteralType) column.Column {
	return column.NewColumn(column.NewLiteral(value))
}

func Int8Lit(value int8) column.Column {
	return Lit(types.Int8(value))
}

func Int16Lit(value int16) column.Column {
	return Lit(types.Int16(value))
}

func Int32Lit(value int32) column.Column {
	return Lit(types.Int32(value))
}

func Int64Lit(value int64) column.Column {
	return Lit(types.Int64(value))
}

func Float32Lit(value float32) column.Column {
	return Lit(types.Float32(value))
}

func Float64Lit(value float64) column.Column {
	return Lit(types.Float64(value))
}

func StringLit(value string) column.Column {
	return Lit(types.String(value))
}

func BoolLit(value bool) column.Column {
	return Lit(types.Boolean(value))
}

func BinaryLit(value []byte) column.Column {
	return Lit(types.Binary(value))
}

func IntLit(value int) column.Column {
	return Lit(types.Int(value))
}
