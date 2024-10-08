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
	// At returns field's value at the given index within a [Row].
	// It returns nil for invalid indices.
	At(index int) any
	// Value returns field's value of the given column's name within a [Row].
	// It returns nil for invalid column's name.
	Value(name string) any
	// Values returns values of all fields within a [Row] as a slice of any.
	Values() []any
	// Len returns the number of fields within a [Row]
	Len() int
	FieldNames() []string
}

type rowImpl struct {
	values  []any
	offsets map[string]int
}

func (r *rowImpl) At(index int) any {
	if index < 0 || index > len(r.values) {
		return nil
	}
	return r.values[index]
}

func (r *rowImpl) Value(name string) any {
	idx, ok := r.offsets[name]
	if !ok {
		return nil
	}
	return r.values[idx]
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
