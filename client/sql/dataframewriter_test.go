package sql

import (
	proto "github.com/apache/spark-connect-go/v1/internal/generated"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSaveMode(t *testing.T) {
	mode, err := getSaveMode("")
	assert.Nil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_UNSPECIFIED, mode)

	mode, err = getSaveMode("append")
	assert.Nil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_APPEND, mode)

	mode, err = getSaveMode("Overwrite")
	assert.Nil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_OVERWRITE, mode)

	mode, err = getSaveMode("ErrorIfExists")
	assert.Nil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_ERROR_IF_EXISTS, mode)

	mode, err = getSaveMode("IGNORE")
	assert.Nil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_IGNORE, mode)

	mode, err = getSaveMode("XYZ")
	assert.NotNil(t, err)
	assert.Equal(t, proto.WriteOperation_SAVE_MODE_UNSPECIFIED, mode)
}
