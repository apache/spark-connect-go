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
}

type BooleanType struct{}

func (t BooleanType) TypeName() string {
	return getDataTypeName(t)
}

type ByteType struct{}

func (t ByteType) TypeName() string {
	return getDataTypeName(t)
}

type ShortType struct{}

func (t ShortType) TypeName() string {
	return getDataTypeName(t)
}

type IntegerType struct{}

func (t IntegerType) TypeName() string {
	return getDataTypeName(t)
}

type LongType struct{}

func (t LongType) TypeName() string {
	return getDataTypeName(t)
}

type FloatType struct{}

func (t FloatType) TypeName() string {
	return getDataTypeName(t)
}

type DoubleType struct{}

func (t DoubleType) TypeName() string {
	return getDataTypeName(t)
}

type DecimalType struct{}

func (t DecimalType) TypeName() string {
	return getDataTypeName(t)
}

type StringType struct{}

func (t StringType) TypeName() string {
	return getDataTypeName(t)
}

type BinaryType struct{}

func (t BinaryType) TypeName() string {
	return getDataTypeName(t)
}

type TimestampType struct{}

func (t TimestampType) TypeName() string {
	return getDataTypeName(t)
}

type TimestampNtzType struct{}

func (t TimestampNtzType) TypeName() string {
	return getDataTypeName(t)
}

type DateType struct{}

func (t DateType) TypeName() string {
	return getDataTypeName(t)
}

type UnsupportedType struct {
	TypeInfo any
}

func (t UnsupportedType) TypeName() string {
	return getDataTypeName(t)
}

func getDataTypeName(dataType DataType) string {
	typeName := fmt.Sprintf("%T", dataType)
	nonQualifiedTypeName := strings.Split(typeName, ".")[1]
	return strings.TrimSuffix(nonQualifiedTypeName, "Type")
}
