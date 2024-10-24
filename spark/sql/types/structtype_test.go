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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructOf(t *testing.T) {
	s := StructOf(NewStructField("col1", BYTE))
	assert.Len(t, s.Fields, 1)
}

func TestTreeString(t *testing.T) {
	c := NewStructField("col1", STRING)
	c.Nullable = false
	s := StructOf(
		c,
		NewStructField("col2", INTEGER),
		NewStructField("col3", DATE),
	)
	assert.Len(t, s.Fields, 3)
	ts := s.TreeString()
	assert.Contains(t, ts, "|-- col1: string (nullable = false")
	assert.Contains(t, ts, "|-- col2: integer (nullable = true)")
	assert.Contains(t, ts, "|-- col3: date (nullable = true)")
}
