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

type Row interface {
	At(index int) any
	Value(name string) any
	Values() []any
	Len() int
	FieldNames() []string
}

type rowImpl struct {
	values  []any
	offsets map[string]int
}

func (r *rowImpl) At(index int) any {
	return r.values[index]
}

func (r *rowImpl) Value(name string) any {
	return r.values[r.offsets[name]]
}

func (r *rowImpl) Values() []any {
	return r.values
}

func (r *rowImpl) Len() int {
	return len(r.values)
}

func (r *rowImpl) FieldNames() []string {
	names := make([]string, 0, len(r.offsets))
	for name := range r.offsets {
		names = append(names, name)
	}
	return names
}
