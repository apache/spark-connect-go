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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuiltinTypes(t *testing.T) {
	p, err := Int8(1).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetByte(), int32(1))

	p, err = Int16(1).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetShort(), int32(1))

	p, err = Int32(1).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetInteger(), int32(1))

	p, err = Int64(1).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetLong(), int64(1))

	p, err = Float32(1.0).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetFloat(), float32(1.0))

	p, err = Float64(1.0).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetDouble(), float64(1.0))

	p, err = String("1").ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetString_(), "1")

	p, err = Boolean(true).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetBoolean(), true)

	p, err = Binary([]byte{1}).ToProto(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, p.GetLiteral().GetBinary(), []byte{1})
}

func testMe(n NumericLiteral) bool {
	return true
}

func testPrimitive(p PrimitiveTypeLiteral) bool {
	return true
}

func TestNumericTypes(t *testing.T) {
	assert.True(t, testMe(Int8(1)))
	assert.True(t, testMe(Int16(1)))
	assert.True(t, testMe(Int32(1)))
	assert.True(t, testMe(Int64(1)))
	assert.True(t, testMe(Float32(1.0)))
	assert.True(t, testMe(Float64(1.0)))

	assert.True(t, testPrimitive(String("a")))
	assert.True(t, testPrimitive(Boolean(true)))
	assert.True(t, testPrimitive(Int16(1)))
}
