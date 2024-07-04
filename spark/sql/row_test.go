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
