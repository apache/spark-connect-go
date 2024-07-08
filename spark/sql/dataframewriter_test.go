package sql

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"

	"github.com/apache/spark-connect-go/v35/spark/client"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/mocks"
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
		client: client.NewExecutePlanClient(&mocks.ProtoClient{
			RecvResponse: []*mocks.MockResponse{
				{
					Err: io.EOF,
				},
			},
		}, uuid.NewString()),
	}
	session := &sparkSessionImpl{
		client:    executor,
		sessionId: uuid.NewString(),
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(session, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.NoError(t, err)
}

func TestSaveFailsIfAnotherErrorHappensWhenReadingStream(t *testing.T) {
	relation := &proto.Relation{}
	executor := &testExecutor{
		client: client.NewExecutePlanClient(&mocks.ProtoClient{
			RecvResponse: []*mocks.MockResponse{
				{
					Err: assert.AnError,
				},
			},
		}, uuid.NewString()),
	}
	session := &sparkSessionImpl{
		client:    executor,
		sessionId: uuid.NewString(),
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(session, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.Error(t, err)
}

func TestSaveFailsIfAnotherErrorHappensWhenExecuting(t *testing.T) {
	relation := &proto.Relation{}
	executor := &testExecutor{
		client: client.NewExecutePlanClient(&mocks.ProtoClient{}, uuid.NewString()),
		err:    assert.AnError,
	}
	session := &sparkSessionImpl{
		client: executor,
	}
	ctx := context.Background()
	path := "path"

	writer := newDataFrameWriter(session, relation)
	writer.Format("format")
	writer.Mode("append")
	err := writer.Save(ctx, path)
	assert.Error(t, err)
}
