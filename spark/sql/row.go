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

// Row represents a row in a DataFrame.
type Row interface {
	// Schema returns the schema of the row.
	Schema() (*StructType, error)
	// Values returns the values of the row.
	Values() ([]any, error)
}

// genericRowWithSchema represents a row in a DataFrame with schema.
type genericRowWithSchema struct {
	values []any
	schema *StructType
}

func NewRowWithSchema(values []any, schema *StructType) Row {
	return &genericRowWithSchema{
		values: values,
		schema: schema,
	}
}

func (r *genericRowWithSchema) Schema() (*StructType, error) {
	return r.schema, nil
}

func (r *genericRowWithSchema) Values() ([]any, error) {
	return r.values, nil
}
