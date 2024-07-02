package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadCreatesADataFrame(t *testing.T) {
	reader := newDataframeReader(nil)
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
	relation := toRelation(path, formatSource)
	assert.NotNil(t, relation)
	assert.Equal(t, &formatSource, relation.GetRead().GetDataSource().Format)
	assert.Equal(t, path, relation.GetRead().GetDataSource().Paths[0])
}
