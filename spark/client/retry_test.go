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

package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v40/spark/client/options"

	"github.com/apache/spark-connect-go/v40/spark/client/testutils"
	"github.com/apache/spark-connect-go/v40/spark/mocks"
	"github.com/apache/spark-connect-go/v40/spark/sparkerrors"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func alwaysRetry(err error) bool {
	return true
}

func Test_retryState_nextAttempt(t *testing.T) {
	basePolicy := RetryPolicy{
		MaxRetries:         15,
		MaxBackoff:         60 * time.Second,
		InitialBackoff:     100 * time.Millisecond,
		BackoffMultiplier:  4,
		Jitter:             time.Millisecond * 100,
		MinJitterThreshold: 2 * time.Second,
		Name:               "BasePolicy",
		Handler:            alwaysRetry,
	}

	type fields struct {
		retryCount int32
		nextWait   time.Duration
	}
	type args struct {
		p RetryPolicy
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantLower time.Duration
		wantUpper time.Duration
		exceeded  bool
	}{
		{
			"BasicRetry - initial backoff",
			fields{
				0,
				0,
			},
			args{
				basePolicy,
			},
			100 * time.Millisecond,
			0,
			false,
		},
		{
			"Jitter applied correctly",
			fields{
				1,
				3 * time.Second,
			},
			args{
				basePolicy,
			},
			3 * time.Second,
			3*time.Second + basePolicy.Jitter,
			false,
		},

		{
			"Retries Exceeded",
			fields{
				16,
				0,
			},
			args{
				basePolicy,
			},
			0,
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &retryState{
				retryCount: tt.fields.retryCount,
				nextWait:   tt.fields.nextWait,
			}
			if tt.exceeded {
				assert.Nilf(t, rs.nextAttempt(tt.args.p),
					"Expecting retries to be exceeded (%v, %v)", rs, tt.args.p)
			} else {
				val := *rs.nextAttempt(tt.args.p)
				if tt.wantUpper > 0 {
					assert.LessOrEqualf(t, tt.wantLower, val, "nextAttempt(%v, %v)", rs, tt.args.p)
					assert.GreaterOrEqualf(t, tt.wantUpper, val, "nextAttempt(%v, %v)", rs, tt.args.p)
				} else {
					assert.Equalf(t, tt.wantLower, val, "nextAttempt(%v, %v)", rs, tt.args.p)
				}
			}
		})
	}
}

func Test_retryMaxBackOff_applied(t *testing.T) {
	basePolicy := RetryPolicy{
		MaxRetries:         15,
		MaxBackoff:         60 * time.Second,
		InitialBackoff:     100 * time.Millisecond,
		BackoffMultiplier:  4,
		Jitter:             time.Millisecond * 100,
		MinJitterThreshold: 2 * time.Second,
		Name:               "BasePolicy",
		Handler:            alwaysRetry,
	}

	state := retryState{
		retryCount: 3,
		nextWait:   30 * time.Second,
	}

	wait := state.nextAttempt(basePolicy)
	assert.LessOrEqualf(t, 30*time.Second, *wait, " nowWait: nextAttempt(%v, %v)", state, basePolicy)
	assert.GreaterOrEqualf(t, 30*time.Second+basePolicy.Jitter, *wait,
		" nowWait: nextAttempt(%v, %v)", state, basePolicy)
	assert.Equalf(t, 60*time.Second, state.nextWait, " nextWait: nextAttempt(%v, %v)", state, basePolicy)
}

func Test_defaultRetryPolicyBehavior(t *testing.T) {
	state := retryState{
		0,
		0,
	}

	w := state.nextAttempt(DefaultRetryPolicy)
	assert.NotNil(t, w)
	assert.Equal(t, DefaultRetryPolicy.InitialBackoff, *w)

	// Check the next iterations until failure
	w = state.nextAttempt(DefaultRetryPolicy)
	assert.NotNil(t, w)
	expected := time.Duration(int64(float32(DefaultRetryPolicy.InitialBackoff.Milliseconds())*
		DefaultRetryPolicy.BackoffMultiplier)) * time.Millisecond
	assert.GreaterOrEqual(t, expected, *w)

	for i := int32(2); i < DefaultRetryPolicy.MaxRetries; i++ {
		w = state.nextAttempt(DefaultRetryPolicy)
		assert.NotNil(t, w)
		assert.LessOrEqualf(t, *w, 60*time.Second+DefaultRetryPolicy.Jitter,
			"nextAttempt(%v, %v)", state, DefaultRetryPolicy)
	}
	// Check that the next attempt is nil
	w = state.nextAttempt(DefaultRetryPolicy)
	assert.Nil(t, w)
}

func Test_default_retryHandler(t *testing.T) {
	err := io.EOF
	assert.Falsef(t, DefaultRetryPolicy.Handler(err), "Must not retry other errors")
	err = errors.New("Some error")
	assert.Falsef(t, DefaultRetryPolicy.Handler(err), "Must not retry other errors")
	s := status.New(codes.Unavailable, "Unavailable")
	assert.Truef(t, DefaultRetryPolicy.Handler(s.Err()), "Must retry Unavailable")
	s = status.New(codes.Internal, "ANALYSIS EXCEPTION")
	assert.Falsef(t, DefaultRetryPolicy.Handler(s.Err()), "Must not retry Internal")
	s = status.New(codes.Internal, "Error: INVALID_CURSOR.DISCONNECTED")
	assert.Truef(t, DefaultRetryPolicy.Handler(s.Err()),
		"Must retry Internal with INVALID_CURSOR.DISCONNECTED")
}

func Test_retriable_success(t *testing.T) {
	toRetry := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
	)
	responseStream := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	c := testutils.NewConnectServiceClientMock(responseStream, nil, nil, t)
	stream := retriableExecutePlanClient{
		context: context.Background(),
		retryContext: &retryContext{
			stream:        toRetry,
			client:        c,
			retryPolicies: []RetryPolicy{TestingRetryPolicy},
		},
	}
	_, err := stream.Recv()
	assert.NoError(t, err)
}

func Test_retriable_client(t *testing.T) {
	toRetry := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
		&mocks.ExecutePlanResponseUnavailable,
	)

	responseStream := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	c := testutils.NewConnectServiceClientMock(responseStream, nil, nil, t)
	stream := retriableExecutePlanClient{
		context: context.Background(),
		retryContext: &retryContext{
			stream: toRetry,
			client: c,
		},
	}

	_, err := stream.Recv()
	assert.ErrorIs(t, err, sparkerrors.RetriesExceeded)

	c = testutils.NewConnectServiceClientMock(toRetry, nil, nil, t)
	stream = retriableExecutePlanClient{
		context: context.Background(),
		retryContext: &retryContext{
			stream:        toRetry,
			client:        c,
			retryPolicies: []RetryPolicy{TestingRetryPolicy},
		},
	}

	_, err = stream.Recv()
	assert.ErrorIs(t, err, sparkerrors.RetriesExceeded)
}

func Test_retriable_with_reattach(t *testing.T) {
	//
	toRetry := mocks.NewProtoClientMock(
		&mocks.ExecutePlanResponseWithSchema,
		&mocks.ExecutePlanResponseUnavailable,
	)

	// Final response stream.
	responseStream := mocks.NewProtoClientMock(
		// First let's do another round of retry and then complete.
		&mocks.ExecutePlanResponseUnavailable,
		// Now, finsih the stream successfully
		&mocks.ExecutePlanResponseDone,
		&mocks.ExecutePlanResponseEOF)

	c := testutils.NewConnectServiceClientMock(responseStream, nil, nil, t)
	client := retriableSparkConnectClient{
		client:        c,
		sessionId:     mocks.MockSessionId,
		retryPolicies: []RetryPolicy{TestingRetryPolicy},
		options:       options.DefaultSparkClientOptions,
	}

	stream := retriableExecutePlanClient{
		context: context.Background(),
		retryContext: &retryContext{
			stream:        toRetry,
			client:        &client,
			request:       &mocks.ExecutePlanRequestSql,
			retryPolicies: []RetryPolicy{TestingRetryPolicy},
		},
	}

	// Fetch the first response.
	_, err := stream.Recv()
	assert.NoError(t, err)

	_, err = stream.Recv()
	assert.NoError(t, err)
}

func Test_client_retriable_basics_execute(t *testing.T) {
	stream := mocks.NewProtoClientMock(&mocks.ExecutePlanResponseDone, &mocks.ExecutePlanResponseEOF)
	c := testutils.NewConnectServiceClientMock(stream, nil, nil, t)
	client := retriableSparkConnectClient{
		client:        c,
		sessionId:     mocks.MockSessionId,
		retryPolicies: []RetryPolicy{TestingRetryPolicy},
		options:       options.DefaultSparkClientOptions,
	}
	ctx := context.Background()
	stream, err := client.ExecutePlan(ctx, &mocks.ExecutePlanRequestSql)
	assert.NoError(t, err)
	assert.NotNil(t, stream)

	_, err = stream.Recv()
	assert.NoError(t, err)

	_, err = stream.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

func Test_client_retriable_basics_analyze(t *testing.T) {
	c := testutils.NewConnectServiceClientMock(nil, mocks.AnalyzePlanResponse, nil, t)
	client := retriableSparkConnectClient{
		client:        c,
		sessionId:     mocks.MockSessionId,
		retryPolicies: []RetryPolicy{TestingRetryPolicy},
		options:       options.DefaultSparkClientOptions,
	}
	ctx := context.Background()
	resp, err := client.AnalyzePlan(ctx, &mocks.AnalyzePlanRequestSql)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, mocks.MockSessionId, resp.SessionId)
}
