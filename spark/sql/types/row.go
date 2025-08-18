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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

type Row interface {
	// At returns field's value at the given index within a [Row].
	// It returns nil for invalid indices.
	At(index int) any
	// Value returns field's value of the given column's name within a [Row].
	// It returns nil for invalid column's name.
	Value(name string) any
	// Values returns values of all fields within a [Row] as a slice of any.
	Values() []any
	// Len returns the number of fields within a [Row].
	Len() int
	FieldNames() []string
	// ToJsonString converts the Row to a JSON string representation.
	// Returns an error if the row contains data that cannot be properly represented in JSON.
	ToJsonString() (string, error)
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
	names := make([]string, len(r.offsets))
	// Sort the field names to make the output deterministic.
	for k, v := range maps.All(r.offsets) {
		names[v] = k
	}
	return names
}

func (r *rowImpl) ToJsonString() (string, error) {
	jsonMap := make(map[string]any)
	fieldNames := r.FieldNames()

	for i, fieldName := range fieldNames {
		value := r.values[i]
		convertedValue, err := convertToJsonValue(value)
		if err != nil {
			return "", fmt.Errorf("failed to convert field '%s': %w", fieldName, err)
		}
		jsonMap[fieldName] = convertedValue
	}

	jsonBytes, err := json.Marshal(jsonMap)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return string(jsonBytes), nil
}

func convertToJsonValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case bool, string, int8, int16, int32, int64, float32, float64:
		return v, nil

	case []byte:
		return base64.StdEncoding.EncodeToString(v), nil

	case decimal128.Num:
		return v.BigInt().String(), nil

	case decimal256.Num:
		return v.BigInt().String(), nil

	case arrow.Timestamp:
		epochUs := int64(v)
		t := time.Unix(epochUs/1000000, (epochUs%1000000)*1000).UTC()
		return t.Format(time.RFC3339), nil

	case arrow.Date32:
		epochDays := int64(v)
		epochTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(epochDays))
		return epochTime.Format("2006-01-02"), nil

	case arrow.Date64:
		epochMs := int64(v)
		t := time.Unix(epochMs/1000, (epochMs%1000)*1000000).UTC()
		return t.Format("2006-01-02"), nil

	case time.Time:
		if v.IsZero() {
			return nil, nil
		}
		return v.Format(time.RFC3339), nil

	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			convertedItem, err := convertToJsonValue(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert array element at index %d: %w", i, err)
			}
			result[i] = convertedItem
		}
		return result, nil

	case map[any]any:
		result := make(map[string]any)
		for key, val := range v {
			keyStr, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("map key must be string for JSON conversion, got %T", key)
			}
			convertedVal, err := convertToJsonValue(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value for key '%s': %w", keyStr, err)
			}
			result[keyStr] = convertedVal
		}
		return result, nil

	case map[string]any:
		result := make(map[string]any)
		for key, val := range v {
			convertedVal, err := convertToJsonValue(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value for key '%s': %w", key, err)
			}
			result[key] = convertedVal
		}
		return result, nil

	default:
		// Use reflection to handle custom types that have basic types as their underlying type.
		// For example, a custom type like "type MyInt int32" would not match the explicit
		// int32 case above, but would match reflect.Int32 here. This ensures we can still
		// convert custom integer, float, bool, and string types to their JSON representations.
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			length := rv.Len()
			result := make([]any, length)
			for i := 0; i < length; i++ {
				convertedItem, err := convertToJsonValue(rv.Index(i).Interface())
				if err != nil {
					return nil, fmt.Errorf("failed to convert array element at index %d: %w", i, err)
				}
				result[i] = convertedItem
			}
			return result, nil

		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return nil, fmt.Errorf("map key must be string for JSON conversion, got %s", rv.Type().Key().Kind())
			}
			result := make(map[string]any)
			for _, key := range rv.MapKeys() {
				keyStr := key.String()
				val := rv.MapIndex(key)
				convertedVal, err := convertToJsonValue(val.Interface())
				if err != nil {
					return nil, fmt.Errorf("failed to convert map value for key '%s': %w", keyStr, err)
				}
				result[keyStr] = convertedVal
			}
			return result, nil

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int(), nil

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint(), nil

		case reflect.Float32, reflect.Float64:
			return rv.Float(), nil

		case reflect.Bool:
			return rv.Bool(), nil

		case reflect.String:
			return rv.String(), nil

		default:
			return fmt.Sprintf("%v", value), nil
		}
	}
}
