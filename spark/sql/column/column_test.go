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

package column

import (
	"testing"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/stretchr/testify/assert"
)

func TestColumnFunctions(t *testing.T) {
	col1 := NewColumn(NewColumnReference("col1"))
	col2 := NewColumn(NewColumnReference("col2"))

	tests := []struct {
		name string
		arg  Column
		want *proto.Expression
	}{
		{
			name: "TestNewUnresolvedFunction",
			arg:  NewColumn(NewUnresolvedFunction("id", nil, false)),
			want: &proto.Expression{
				ExprType: &proto.Expression_UnresolvedFunction_{
					UnresolvedFunction: &proto.Expression_UnresolvedFunction{
						FunctionName: "id",
						IsDistinct:   false,
					},
				},
			},
		},
		{
			name: "TestComparison",
			arg:  col1.Lt(col2),
			want: &proto.Expression{
				ExprType: &proto.Expression_UnresolvedFunction_{
					UnresolvedFunction: &proto.Expression_UnresolvedFunction{
						FunctionName: "<",
						IsDistinct:   false,
						Arguments: []*proto.Expression{
							{
								ExprType: &proto.Expression_UnresolvedAttribute_{
									UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
										UnparsedIdentifier: "col1",
									},
								},
							},
							{
								ExprType: &proto.Expression_UnresolvedAttribute_{
									UnresolvedAttribute: &proto.Expression_UnresolvedAttribute{
										UnparsedIdentifier: "col2",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.arg.ToPlan()
			assert.NoError(t, err)
			expected := tt.want
			assert.Equalf(t, expected, got, "Input: %v", tt.arg.Expr.DebugString())
		})
	}
}
