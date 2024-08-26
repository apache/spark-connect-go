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
)

type DataType interface {
	TypeName() string
	IsNumeric() bool
}

type BooleanType struct{}

func (t BooleanType) TypeName() string {
	return getDataTypeName(t)
}

func (t BooleanType) IsNumeric() bool {
	return false
}

type ByteType struct{}

func (t ByteType) IsNumeric() bool {
	return true
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

type IntegerType struct{}

func (t IntegerType) TypeName() string {
	return getDataTypeName(t)
}

func (t IntegerType) IsNumeric() bool {
	return true
}

type LongType struct{}

func (t LongType) TypeName() string {
	return getDataTypeName(t)
}

func (t LongType) IsNumeric() bool {
	return true
}

type FloatType struct{}

func (t FloatType) TypeName() string {
	return getDataTypeName(t)
}

func (t FloatType) IsNumeric() bool {
	return true
}

type DoubleType struct{}

func (t DoubleType) TypeName() string {
	return getDataTypeName(t)
}

func (t DoubleType) IsNumeric() bool {
	return true
}

type DecimalType struct{}

func (t DecimalType) TypeName() string {
	return getDataTypeName(t)
}

func (t DecimalType) IsNumeric() bool {
	return true
}

type StringType struct{}

func (t StringType) TypeName() string {
	return getDataTypeName(t)
}

func (t StringType) IsNumeric() bool {
	return false
}

type BinaryType struct{}

func (t BinaryType) TypeName() string {
	return getDataTypeName(t)
}

func (t BinaryType) IsNumeric() bool {
	return false
}

type TimestampType struct{}

func (t TimestampType) TypeName() string {
	return getDataTypeName(t)
}

func (t TimestampType) IsNumeric() bool {
	return false
}

type TimestampNtzType struct{}

func (t TimestampNtzType) TypeName() string {
	return getDataTypeName(t)
}

func (t TimestampNtzType) IsNumeric() bool {
	return false
}

type DateType struct{}

func (t DateType) TypeName() string {
	return getDataTypeName(t)
}

func (t DateType) IsNumeric() bool {
	return false
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

func getDataTypeName(dataType DataType) string {
	typeName := fmt.Sprintf("%T", dataType)
	nonQualifiedTypeName := strings.Split(typeName, ".")[1]
	return strings.TrimSuffix(nonQualifiedTypeName, "Type")
}
