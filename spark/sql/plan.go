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
	"sync/atomic"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

var atomicInt64 atomic.Int64

func newPlanId() *int64 {
	v := atomicInt64.Add(1)
	return &v
}

func resetPlanIdForTesting() {
	atomicInt64.Swap(0)
}

func newReadTableRelation(table string) *proto.Relation {
	return &proto.Relation{
		Common: &proto.RelationCommon{
			PlanId: newPlanId(),
		},
		RelType: &proto.Relation_Read{
			Read: &proto.Read{
				ReadType: &proto.Read_NamedTable_{
					NamedTable: &proto.Read_NamedTable{
						UnparsedIdentifier: table,
					},
				},
			},
		},
	}
}

func newReadWithFormatAndPath(path, format string) *proto.Relation {
	return &proto.Relation{
		RelType: &proto.Relation_Read{
			Read: &proto.Read{
				ReadType: &proto.Read_DataSource_{
					DataSource: &proto.Read_DataSource{
						Format: &format,
						Paths:  []string{path},
					},
				},
			},
		},
	}
}

func newReadWithFormatAndPathAndOptions(path, format string, options map[string]string) *proto.Relation {
	return &proto.Relation{
		RelType: &proto.Relation_Read{
			Read: &proto.Read{
				ReadType: &proto.Read_DataSource_{
					DataSource: &proto.Read_DataSource{
						Format:  &format,
						Paths:   []string{path},
						Options: options,
					},
				},
			},
		},
	}
}
