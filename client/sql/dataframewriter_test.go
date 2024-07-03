package sql

import (
	"context"
	"io"
	"testing"

	proto "github.com/apache/spark-connect-go/v3.5/internal/generated"
	"github.com/stretchr/testify/assert"
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

func TestSaveExecutesWriteOperationUntilEOF(t *testing.T) {
	relation := &proto.Relation{}
	executor := &testExecutor{
		client: newExecutePlanClient(&protoClient{
			err: io.EOF,
		}),
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(executor, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.NoError(t, err)
}

func TestSaveFailsIfAnotherErrorHappensWhenReadingStream(t *testing.T) {
	relation := &proto.Relation{}
	executor := &testExecutor{
		client: newExecutePlanClient(&protoClient{
			err: assert.AnError,
		}),
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(executor, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.Error(t, err)
}

func TestSaveFailsIfAnotherErrorHappensWhenExecuting(t *testing.T) {
	relation := &proto.Relation{}
	executor := &testExecutor{
		client: newExecutePlanClient(&protoClient{}),
		err:    assert.AnError,
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(executor, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.Error(t, err)
}
