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

package types

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

type DataType interface {
	TypeName() string
	IsNumeric() bool
	ToArrowType() arrow.DataType
}

type BooleanType struct{}

func (t BooleanType) TypeName() string {
	return getDataTypeName(t)
}

func (t BooleanType) IsNumeric() bool {
	return false
}

func (t BooleanType) ToArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Boolean
}

type ByteType struct{}

func (t ByteType) IsNumeric() bool {
	return true
}

func (t ByteType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int8
}

func (t ByteType) TypeName() string {
	return getDataTypeName(t)
}

type ShortType struct{}

func (t ShortType) TypeName() string {
	return getDataTypeName(t)
}

func (t ShortType) IsNumeric() bool {
	return true
}

func (t ShortType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int16
}

type IntegerType struct{}

func (t IntegerType) TypeName() string {
	return getDataTypeName(t)
}

func (t IntegerType) IsNumeric() bool {
	return true
}

func (t IntegerType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int32
}

type LongType struct{}

func (t LongType) TypeName() string {
	return getDataTypeName(t)
}

func (t LongType) IsNumeric() bool {
	return true
}

func (t LongType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

type FloatType struct{}

func (t FloatType) TypeName() string {
	return getDataTypeName(t)
}

func (t FloatType) IsNumeric() bool {
	return true
}

func (t FloatType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float32
}

type DoubleType struct{}

func (t DoubleType) TypeName() string {
	return getDataTypeName(t)
}

func (t DoubleType) IsNumeric() bool {
	return true
}

func (t DoubleType) ToArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

type DecimalType struct {
	Precision int32
	Scale     int32
}

func (t DecimalType) TypeName() string {
	return getDataTypeName(t)
}

func (t DecimalType) IsNumeric() bool {
	return true
}

func (t DecimalType) ToArrowType() arrow.DataType {
	return &arrow.Decimal128Type{
		Precision: t.Precision,
		Scale:     t.Scale,
	}
}

type StringType struct{}

func (t StringType) TypeName() string {
	return getDataTypeName(t)
}

func (t StringType) IsNumeric() bool {
	return false
}

func (t StringType) ToArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

type BinaryType struct{}

func (t BinaryType) TypeName() string {
	return getDataTypeName(t)
}

func (t BinaryType) IsNumeric() bool {
	return false
}

func (t BinaryType) ToArrowType() arrow.DataType {
	return arrow.BinaryTypes.Binary
}

type TimestampType struct{}

func (t TimestampType) TypeName() string {
	return getDataTypeName(t)
}

func (t TimestampType) IsNumeric() bool {
	return false
}

func (t TimestampType) ToArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Timestamp_ms
}

type TimestampNtzType struct{}

func (t TimestampNtzType) TypeName() string {
	return getDataTypeName(t)
}

func (t TimestampNtzType) IsNumeric() bool {
	return false
}

func (t TimestampNtzType) ToArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Timestamp_ns
}

type DateType struct{}

func (t DateType) TypeName() string {
	return getDataTypeName(t)
}

func (t DateType) IsNumeric() bool {
	return false
}

func (t DateType) ToArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Date32
}

type UnsupportedType struct {
	TypeInfo any
}

func (t UnsupportedType) TypeName() string {
	return getDataTypeName(t)
}

func (t UnsupportedType) IsNumeric() bool {
	return false
}

func (t UnsupportedType) ToArrowType() arrow.DataType {
	return nil
}

func getDataTypeName(dataType DataType) string {
	typeName := fmt.Sprintf("%T", dataType)
	nonQualifiedTypeName := strings.Split(typeName, ".")[1]
	return strings.TrimSuffix(nonQualifiedTypeName, "Type")
}

var (
	BOOLEAN       = BooleanType{}
	BYTE          = ByteType{}
	SHORT         = ShortType{}
	INTEGER       = IntegerType{}
	LONG          = LongType{}
	FLOAT         = FloatType{}
	DOUBLE        = DoubleType{}
	DATE          = DateType{}
	TIMESTAMP     = TimestampType{}
	TIMESTAMP_NTZ = TimestampNtzType{}
	STRING        = StringType{}
)
