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

package channel_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/apache/spark-connect-go/v40/spark/client/channel"
	"github.com/apache/spark-connect-go/v40/spark/sparkerrors"
	"github.com/stretchr/testify/assert"
)

const goodChannelURL = "sc://host:15002/;user_id=a;token=b;x-other-header=c"

func TestBasicChannelBuilder(t *testing.T) {
	cb, _ := channel.NewBuilder(goodChannelURL)
	if cb == nil {
		t.Error("ChannelBuilder must not be null")
	}
}

func TestBasicChannelParsing(t *testing.T) {
	_, err := channel.NewBuilder("abc://asdada:1333")
	assert.False(t, strings.Contains(err.Error(), "scheme"),
		"Channel build should fail with wrong scheme")

	_, err = channel.NewBuilder("sc://:1333")
	assert.False(t, strings.Contains(err.Error(), "scheme"),
		"Should not have an error for a proper URL")

	cb, err := channel.NewBuilder("sc://empty")
	assert.Nilf(t, err, "Valid path should not fail: %v", err)
	assert.Equalf(t, 15002, cb.Port(), "Default port must be set, but got %v", cb.Port)

	_, err = channel.NewBuilder("sc://empty:port")
	assert.NotNilf(t, err, "Port must be a valid integer %v", err)

	_, err = channel.NewBuilder("sc://empty:9999999999999")
	assert.Nilf(t, err, "Port must be a valid number %v", err)

	_, err = channel.NewBuilder("sc://abcd/this")
	assert.True(t, strings.Contains(err.Error(), "URL path"), "URL path elements are not allowed")
	assert.ErrorIs(t, err, sparkerrors.InvalidInputError)

	cb, err = channel.NewBuilder(goodChannelURL)
	assert.Nilf(t, err, "Should not have an error for a proper URL")
	assert.Equal(t, "host", cb.Host())
	assert.Equal(t, 15002, cb.Port())
	assert.Len(t, cb.Headers(), 1)
	assert.Equal(t, "c", cb.Headers()["x-other-header"])
	assert.Equal(t, "a", cb.User())
	assert.Equal(t, "b", cb.Token())

	cb, err = channel.NewBuilder("sc://localhost:443/;token=token;user_id=user_id;cluster_id=a;session_id=session")
	assert.NoError(t, err)
	assert.Equal(t, 443, cb.Port())
	assert.Equal(t, "localhost", cb.Host())
	assert.Equal(t, "token", cb.Token())
	assert.Equal(t, "user_id", cb.User())
	assert.Equal(t, "session", cb.SessionId())
}

func TestChannelBuildConnect(t *testing.T) {
	ctx := context.Background()
	cb, err := channel.NewBuilder("sc://localhost")
	assert.NoError(t, err)
	id := cb.SessionId()
	_, err = uuid.Parse(id)
	assert.NoError(t, err)
	assert.NoError(t, err, "Should not have an error for a proper URL.")
	conn, err := cb.Build(ctx)
	assert.Nil(t, err, "no error for proper connection")
	assert.NotNil(t, conn)

	cb, err = channel.NewBuilder("sc://localhost:443/;token=abcd;user_id=a")
	assert.Nil(t, err, "Should not have an error for a proper URL.")
	conn, err = cb.Build(ctx)
	assert.Nil(t, err, "no error for proper connection")
	assert.NotNil(t, conn)
}

func TestChannelBulder_UserAgent(t *testing.T) {
	cb, err := channel.NewBuilder("sc://localhost")
	assert.NoError(t, err)
	assert.True(t, strings.Contains(cb.UserAgent(), "_SPARK_CONNECT_GO"))
	assert.True(t, strings.Contains(cb.UserAgent(), "go/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "spark/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "os/"))

	cb, err = channel.NewBuilder("sc://localhost/;user_agent=custom")
	assert.NoError(t, err)
	assert.True(t, strings.Contains(cb.UserAgent(), "custom"))
	assert.True(t, strings.Contains(cb.UserAgent(), "go/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "spark/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "os/"))
}
