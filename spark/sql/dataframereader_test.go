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

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadCreatesADataFrame(t *testing.T) {
	reader := NewDataframeReader(nil)
	source := "source"
	path := "path"
	reader.Format(source)
	frame, err := reader.Load(path)
	assert.NoError(t, err)
	assert.NotNil(t, frame)
}

func TestRelationContainsPathAndFormat(t *testing.T) {
	formatSource := "source"
	path := "path"
	relation := newReadWithFormatAndPath(path, formatSource)
	assert.NotNil(t, relation)
	assert.Equal(t, &formatSource, relation.GetRead().GetDataSource().Format)
	assert.Equal(t, path, relation.GetRead().GetDataSource().Paths[0])
}
