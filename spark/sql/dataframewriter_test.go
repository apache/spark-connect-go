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

package sql

import (
	"context"
	"testing"

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
	executor := client.NewTestConnectClientFromResponses(mocks.MockSessionId,
		&mocks.ExecutePlanResponseDone, &mocks.ExecutePlanResponseEOF)
	session := &sparkSessionImpl{
		client:    executor,
		sessionId: mocks.MockSessionId,
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
	executor := client.NewTestConnectClientFromResponses(mocks.MockSessionId, &mocks.MockResponse{
		Err: assert.AnError,
	})
	session := &sparkSessionImpl{
		client:    executor,
		sessionId: mocks.MockSessionId,
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
	executor := client.NewTestConnectClientWithImmediateError(mocks.MockSessionId, assert.AnError)
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
