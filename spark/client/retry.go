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
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/client/base"

	"github.com/apache/spark-connect-go/v35/spark/client/options"
	"google.golang.org/grpc/metadata"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type RetryHandler func(error) bool

// RetryPolicy defines the parameters for a retry policy. The policy is used to determine if an
// error is retriable and how to handle retries. The policy defines the behavior of the client
// in how it backs off in case of an error and how the retries are spread out over time.
type RetryPolicy struct {
	MaxRetries         int32
	InitialBackoff     time.Duration
	MaxBackoff         time.Duration
	BackoffMultiplier  float32
	Jitter             time.Duration
	MinJitterThreshold time.Duration
	Name               string
	Handler            RetryHandler
}

// DefaultRetryPolicy is the default retry policy used by the client. It will retry on Unavailable and
// in case the cursor has been disconnected. All other errors are considered to be not retriable.
var DefaultRetryPolicy = RetryPolicy{
	MaxRetries:         15,
	InitialBackoff:     50 * time.Millisecond,
	MaxBackoff:         1 * time.Minute,
	BackoffMultiplier:  4.0,
	Jitter:             500 * time.Millisecond,
	MinJitterThreshold: 2000 * time.Millisecond,
	Name:               "DefaultRetryPolicy",
	Handler: func(e error) bool {
		status := sparkerrors.FromRPCError(e)
		switch status.Code {
		case codes.Unavailable:
			return true
		case codes.Internal:
			if strings.Contains(status.Message, "INVALID_CURSOR.DISCONNECTED") {
				return true
			}
		}
		return false
	},
}

var TestingRetryPolicy = RetryPolicy{
	MaxRetries:         5,
	InitialBackoff:     0,
	MaxBackoff:         1,
	BackoffMultiplier:  2,
	Jitter:             0,
	MinJitterThreshold: 0,
	Name:               "TestingRetryPolicy",
	Handler: func(e error) bool {
		status := sparkerrors.FromRPCError(e)
		switch status.Code {
		case codes.Unavailable:
			return true
		case codes.Internal:
			if strings.Contains(status.Message, "INVALID_CURSOR.DISCONNECTED") {
				return true
			}
		}
		return false
	},
}

// DefaultRetryPolicyRegistry is the default set of retry policies used by the client. It contains
// all those policies that are enabled by default.
var DefaultRetryPolicyRegistry = []RetryPolicy{DefaultRetryPolicy}

// retryState is the current state of the retries for one particular RPC request. The retry
// state is independent of the retry policy.
type retryState struct {
	retryCount int32
	nextWait   time.Duration
}

func (rs *retryState) nextAttempt(p RetryPolicy) *time.Duration {
	if rs.retryCount >= p.MaxRetries {
		return nil
	}

	// For the first retry pick the initial backoff of the matching policy.
	if rs.retryCount == 0 {
		rs.nextWait = p.InitialBackoff
	}

	// Adjust the retry count and calculate the next wait.
	rs.retryCount++
	wait := rs.nextWait
	rs.nextWait = time.Duration(float32(rs.nextWait.Milliseconds())*p.BackoffMultiplier) * time.Millisecond
	if rs.nextWait > p.MaxBackoff {
		rs.nextWait = p.MaxBackoff
	}

	// Some policies define that jitter should only be applied after a particular threshold.
	if wait > p.MinJitterThreshold {
		wait += time.Duration(rand.Float32() * float32(p.Jitter.Milliseconds()))
	}

	return &wait
}

func NewRetriableSparkConnectClient(conn *grpc.ClientConn, sessionId string, opts options.SparkClientOptions) base.SparkConnectRPCClient {
	innerClient := proto.NewSparkConnectServiceClient(conn)
	return &retriableSparkConnectClient{
		client:        innerClient,
		sessionId:     sessionId,
		retryPolicies: DefaultRetryPolicyRegistry,
		options:       opts,
	}
}

// wrapRetriableCall wraps a call to a function that returns a result and an error. The function is
// retried according to the retry policies. The function will return the result or an error if the
// retries are exceeded.
func wrapRetriableCall[Res rpcType](ctx context.Context, retryPolicies []RetryPolicy, in func(context.Context) (Res, error)) (Res, error) {
	var lastErr error
	// Create the retry state for this wrapped call. The retry state captures the information about
	// the wait time and how many retries to perform.
	state := retryState{}
	// As long as the error is retriable, we will retry the operation.
	canRetry := true
	for canRetry {
		// Every loop iteration starts with being non-retriable.
		canRetry = false
		response, lastErr := in(ctx)
		if lastErr != nil {
			for _, h := range retryPolicies {
				if h.Handler(lastErr) {
					canRetry = true
					wait := state.nextAttempt(h)
					if wait != nil {
						time.Sleep(*wait)
					} else {
						// If the retries are exceeded, simply return from here.
						return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
					}
					// Breaks out of the retry handler loop.
					break
				}
			}
		} else {
			// Exit loop if no error has been received.
			return response, nil
		}
	}
	return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
}

type rpcType interface {
	*proto.AnalyzePlanResponse | *proto.ConfigResponse | *proto.ArtifactStatusesResponse | *proto.InterruptResponse | *proto.ReleaseExecuteResponse | *proto.ExecutePlanResponse
}

// retriableSparkConnectClient wraps the SparkConnectServiceClient implementation to
// transparently handle retries.
type retriableSparkConnectClient struct {
	client    base.SparkConnectRPCClient
	sessionId string
	// Not yet used.
	// serverSideSessionId string
	retryPolicies []RetryPolicy
	options       options.SparkClientOptions
}

func (r *retriableSparkConnectClient) ExecutePlan(ctx context.Context, in *proto.ExecutePlanRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ExecutePlanClient, error) {
	var lastErr error
	// Create the retry state for this wrapped call. The retry state captures the information about
	// the wait time and how many retries to perform.
	state := retryState{}
	// As long as the error is retriable, we will retry the operation.
	canRetry := true
	for canRetry {
		// Every loop iteration starts with being non-retriable.
		canRetry = false
		response, lastErr := r.client.ExecutePlan(ctx, in, opts...)
		if lastErr != nil {
			for _, h := range r.retryPolicies {
				if h.Handler(lastErr) {
					canRetry = true
					wait := state.nextAttempt(h)
					if wait != nil {
						time.Sleep(*wait)
					} else {
						// If the retries are exceeded, simply return from here.
						return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
					}
					// Breaks out of the retry handler loop.
					break
				}
			}
		} else {
			// Exit loop if no error has been received.
			rc := retriableExecutePlanClient{
				retryContext: &retryContext{
					stream:         response,
					client:         r.client,
					request:        in,
					resultComplete: false,
					retryPolicies:  r.retryPolicies,
				},
			}
			return rc, nil
		}
	}
	return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
}

func (r *retriableSparkConnectClient) AnalyzePlan(ctx context.Context, in *proto.AnalyzePlanRequest, opts ...grpc.CallOption) (*proto.AnalyzePlanResponse, error) {
	return wrapRetriableCall(ctx, r.retryPolicies, func(ctx2 context.Context) (*proto.AnalyzePlanResponse, error) {
		return r.client.AnalyzePlan(ctx2, in, opts...)
	})
}

func (r *retriableSparkConnectClient) Config(ctx context.Context, in *proto.ConfigRequest, opts ...grpc.CallOption) (*proto.ConfigResponse, error) {
	return wrapRetriableCall(ctx, r.retryPolicies, func(ctx2 context.Context) (*proto.ConfigResponse, error) {
		return r.client.Config(ctx2, in, opts...)
	})
}

func (r *retriableSparkConnectClient) AddArtifacts(ctx context.Context, opts ...grpc.CallOption) (proto.SparkConnectService_AddArtifactsClient, error) {
	var lastErr error
	// Create the retry state for this wrapped call. The retry state captures the information about
	// the wait time and how many retries to perform.
	state := retryState{}
	// As long as the error is retriable, we will retry the operation.
	canRetry := true
	for canRetry {
		// Every loop iteration starts with being non-retriable.
		canRetry = false
		response, lastErr := r.client.AddArtifacts(ctx, opts...)
		if lastErr != nil {
			for _, h := range r.retryPolicies {
				if h.Handler(lastErr) {
					canRetry = true
					wait := state.nextAttempt(h)
					if wait != nil {
						time.Sleep(*wait)
					} else {
						// If the retries are exceeded, simply return from here.
						return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
					}
					// Breaks out of the retry handler loop.
					break
				}
			}
		} else {
			// Exit loop if no error has been received.
			return response, nil
		}
	}
	return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
}

func (r *retriableSparkConnectClient) ArtifactStatus(ctx context.Context, in *proto.ArtifactStatusesRequest, opts ...grpc.CallOption) (*proto.ArtifactStatusesResponse, error) {
	return wrapRetriableCall(ctx, r.retryPolicies, func(ctx2 context.Context) (*proto.ArtifactStatusesResponse, error) {
		return r.client.ArtifactStatus(ctx2, in, opts...)
	})
}

func (r *retriableSparkConnectClient) Interrupt(ctx context.Context, in *proto.InterruptRequest, opts ...grpc.CallOption) (*proto.InterruptResponse, error) {
	return wrapRetriableCall(ctx, r.retryPolicies, func(ctx2 context.Context) (*proto.InterruptResponse, error) {
		return r.client.Interrupt(ctx2, in, opts...)
	})
}

func (r *retriableSparkConnectClient) ReattachExecute(ctx context.Context, in *proto.ReattachExecuteRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ReattachExecuteClient, error) {
	var lastErr error
	// Create the retry state for this wrapped call. The retry state captures the information about
	// the wait time and how many retries to perform.
	state := retryState{}
	// As long as the error is retriable, we will retry the operation.
	canRetry := true
	for canRetry {
		// Every loop iteration starts with being non-retriable.
		canRetry = false
		response, lastErr := r.client.ReattachExecute(ctx, in, opts...)
		if lastErr != nil {
			for _, h := range r.retryPolicies {
				if h.Handler(lastErr) {
					canRetry = true
					wait := state.nextAttempt(h)
					if wait != nil {
						time.Sleep(*wait)
					} else {
						// If the retries are exceeded, simply return from here.
						return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
					}
					// Breaks out of the retry handler loop.
					break
				}
			}
		} else {
			// Exit loop if no error has been received.
			return response, nil
		}
	}
	return nil, sparkerrors.WithType(lastErr, sparkerrors.RetriesExceeded)
}

func (r *retriableSparkConnectClient) ReleaseExecute(ctx context.Context, in *proto.ReleaseExecuteRequest, opts ...grpc.CallOption) (*proto.ReleaseExecuteResponse, error) {
	return wrapRetriableCall(ctx, r.retryPolicies, func(ctx2 context.Context) (*proto.ReleaseExecuteResponse, error) {
		return r.client.ReleaseExecute(ctx2, in, opts...)
	})
}

type retryContext struct {
	stream         proto.SparkConnectService_ExecutePlanClient
	client         base.SparkConnectRPCClient
	request        *proto.ExecutePlanRequest
	lastResponseId *string
	resultComplete bool
	retryPolicies  []RetryPolicy
}

type retriableExecutePlanClient struct {
	retryContext *retryContext
}

func (r retriableExecutePlanClient) Recv() (*proto.ExecutePlanResponse, error) {
	resp, err := r.retryContext.stream.Recv()
	// Success, simply return the result.
	if err == nil {
		r.retryContext.lastResponseId = &resp.ResponseId
		return resp, nil
	}

	// Ignore successful closure.
	if err == io.EOF {
		return nil, err
	}
	ctx := r.retryContext.stream.Context()
	return wrapRetriableCall(ctx, r.retryContext.retryPolicies, func(ctx2 context.Context) (*proto.ExecutePlanResponse, error) {
		// Now we have to assume that the request has failed, and we distinguish two cases: First, we have
		// never received a result and in this case we simply execute the same request again. Second,
		// we will send a reattach request with the same operation ID and the last response ID.
		if r.retryContext.lastResponseId == nil {
			// Send the request again.
			rs, err := r.retryContext.client.ExecutePlan(ctx2, r.retryContext.request)
			if err != nil {
				return nil, err
			}
			switch stream := rs.(type) {
			case retriableExecutePlanClient:
				r.retryContext.stream = stream.retryContext.stream
			default:
				r.retryContext.stream = stream
			}
			return r.retryContext.stream.Recv()
		} else {
			// Send a reattach
			req := &proto.ReattachExecuteRequest{
				SessionId:      r.retryContext.request.SessionId,
				UserContext:    r.retryContext.request.UserContext,
				OperationId:    *r.retryContext.request.OperationId,
				LastResponseId: r.retryContext.lastResponseId,
			}
			re, err := r.retryContext.client.ReattachExecute(ctx2, req)
			if err != nil {
				return nil, err
			}
			switch stream := re.(type) {
			case retriableExecutePlanClient:
				r.retryContext.stream = stream.retryContext.stream
			default:
				r.retryContext.stream = stream
			}
			return r.retryContext.stream.Recv()
		}
	})
}

func (r retriableExecutePlanClient) Header() (metadata.MD, error) {
	return r.retryContext.stream.Header()
}

func (r retriableExecutePlanClient) Trailer() metadata.MD {
	return r.retryContext.stream.Trailer()
}

func (r retriableExecutePlanClient) CloseSend() error {
	return r.retryContext.stream.CloseSend()
}

func (r retriableExecutePlanClient) Context() context.Context {
	return r.retryContext.stream.Context()
}

func (r retriableExecutePlanClient) SendMsg(m any) error {
	return r.retryContext.stream.SendMsg(m)
}

func (r retriableExecutePlanClient) RecvMsg(m any) error {
	return r.retryContext.stream.RecvMsg(m)
}
