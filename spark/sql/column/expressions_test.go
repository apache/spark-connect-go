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
	"reflect"
	"testing"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/stretchr/testify/assert"
)

func TestNewUnresolvedFunction(t *testing.T) {
	colRef := NewColumnReference("martin")
	colRefPlan, _ := colRef.ToPlan()
	type args struct {
		name       string
		arguments  []Expression
		isDistinct bool
	}
	tests := []struct {
		name string
		args args
		want *proto.Expression
	}{
		{
			name: "TestNewUnresolvedFunction",
			args: args{
				name:       "id",
				arguments:  nil,
				isDistinct: false,
			},
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
			name: "TestNewUnresolvedWithArguments",
			args: args{
				name:       "id",
				arguments:  []Expression{colRef},
				isDistinct: false,
			},
			want: &proto.Expression{
				ExprType: &proto.Expression_UnresolvedFunction_{
					UnresolvedFunction: &proto.Expression_UnresolvedFunction{
						FunctionName: "id",
						IsDistinct:   false,
						Arguments: []*proto.Expression{
							colRefPlan,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewUnresolvedFunction(tt.args.name, tt.args.arguments, tt.args.isDistinct).ToPlan()
			assert.NoError(t, err)
			if !reflect.DeepEqual(got, tt.want) {
				assert.Equal(t, tt.want, got)
				t.Errorf("NewUnresolvedFunction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSQLExpression(t *testing.T) {
	type args struct {
		expression string
	}
	tests := []struct {
		name string
		args args
		want *sqlExression
	}{
		{
			name: "TestNewSQLExpression",
			args: args{
				expression: "id < 10",
			},
			want: &sqlExression{
				expression_string: "id < 10",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSQLExpression(tt.args.expression); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSQLExpression() = %v, want %v", got, tt.want)
			}
		})
	}
}
