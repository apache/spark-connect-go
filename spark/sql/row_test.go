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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	values := []any{1}
	schema := &StructType{}
	row := NewRowWithSchema(values, schema)
	schema2, err := row.Schema()
	assert.NoError(t, err)
	assert.Equal(t, schema, schema2)
}

func TestValues(t *testing.T) {
	values := []any{1}
	schema := &StructType{}
	row := NewRowWithSchema(values, schema)
	values2, err := row.Values()
	assert.NoError(t, err)
	assert.Equal(t, values, values2)
}
