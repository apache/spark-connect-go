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

import "github.com/apache/arrow-go/v18/arrow"

// StructField represents a field in a StructType.
type StructField struct {
	Name     string
	DataType DataType
	Nullable bool // default should be true
	Metadata *string
}

func (t *StructField) ToArrowType() arrow.Field {
	return arrow.Field{
		Name:     t.Name,
		Type:     t.DataType.ToArrowType(),
		Nullable: t.Nullable,
	}
}

// StructType represents a struct type.
type StructType struct {
	Fields []StructField
}

func (t *StructType) TypeName() string {
	return "structtype"
}

func (t *StructType) IsNumeric() bool {
	return false
}

func (t *StructType) ToArrowType() *arrow.StructType {
	fields := make([]arrow.Field, len(t.Fields))
	for i, f := range t.Fields {
		fields[i] = f.ToArrowType()
	}
	return arrow.StructOf(fields...)
}

func StructOf(fields ...StructField) *StructType {
	return &StructType{Fields: fields}
}

func NewStructField(name string, dataType DataType) StructField {
	return StructField{
		Name:     name,
		DataType: dataType,
		Nullable: true,
	}
}
