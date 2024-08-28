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

package sparkerrors

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-errors/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type wrappedError struct {
	errorType error
	cause     *errors.Error
}

func (w *wrappedError) Unwrap() []error {
	return []error{w.errorType, w.cause}
}

func (w *wrappedError) Error() string {
	return fmt.Sprintf("%s", w)
}

// WithType wraps an error with a type that can later be checked using `errors.Is`
func WithType(err error, errType errorType) error {
	return &wrappedError{cause: errors.Wrap(err, 1), errorType: errType}
}

func WithString(err error, errMsg string) error {
	return &wrappedError{cause: errors.Wrap(err, 1), errorType: errors.New(errMsg)}
}

func WithStringf(err error, errMsg string, params ...any) error {
	return &wrappedError{cause: errors.Wrap(err, 1), errorType: fmt.Errorf(errMsg, params...)}
}

type errorType error

var (
	ConnectionError               = errorType(errors.New("connection error"))
	ReadError                     = errorType(errors.New("read error"))
	ExecutionError                = errorType(errors.New("execution error"))
	InvalidInputError             = errorType(errors.New("invalid input"))
	InvalidPlanError              = errorType(errors.New("invalid plan"))
	RetriesExceeded               = errorType(errors.New("retries exceeded"))
	InvalidServerSideSessionError = errorType(errors.New("invalid server side session"))
	TestSetupError                = errorType(errors.New("test setup error"))
)

// Format formats the error, supporting both short forms (v, s, q) and verbose form (+v)
func (w *wrappedError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, "[sparkerror] ")
			_, _ = io.WriteString(s, fmt.Sprintf("Error Type: %s\n", w.errorType.Error()))
			_, _ = io.WriteString(s, fmt.Sprintf("Error Cause: %s\n%s", w.cause.Err.Error(), w.cause.Stack()))
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, fmt.Sprintf("%s: %s", w.errorType, w.cause))
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", w.errorType.Error())
	}
}

type UnsupportedResponseTypeError struct {
	ResponseType interface{}
}

func (e UnsupportedResponseTypeError) Error() string {
	return fmt.Sprintf("Received unsupported response type: %T", e.ResponseType)
}

type InvalidServerSideSessionDetailsError struct {
	OwnSessionId      string
	ReceivedSessionId string
}

func (e InvalidServerSideSessionDetailsError) Error() string {
	return fmt.Sprintf("Received invalid session id %s, expected %s", e.ReceivedSessionId, e.OwnSessionId)
}

// SparkError represents an error that is returned from Spark itself. It captures details of the
// error that allows better understanding about the error. This allows us to check if the error
// can be retried or not.
type SparkError struct {
	// SqlState is the SQL state of the error.
	SqlState string
	// ErrorClass is the class of the error.
	ErrorClass string
	// If set is typically the classname throwing the error on the Spark side.
	Reason string
	// Message is the human-readable message of the error.
	Message string
	// Code is the gRPC status code of the error.
	Code codes.Code
	// ErrorId is the unique id of the error. It can be used to fetch more details about
	// the error using an additional RPC from the server.
	ErrorId string
	// Parameters are the parameters that are used to format the error message.
	Parameters map[string]string
	status     *status.Status
}

func (e SparkError) Error() string {
	if e.Code == codes.Internal && e.SqlState != "" {
		return fmt.Sprintf("[%s] %s. SQLSTATE: %s", e.ErrorClass, e.Message, e.SqlState)
	} else {
		return fmt.Sprintf("[%s] %s", e.Code.String(), e.Message)
	}
}

// FromRPCError converts a gRPC error to a SparkError. If the error is not a gRPC error, it will
// create a plain "UNKNOWN" GRPC status type. If no error was observed returns nil.
func FromRPCError(e error) *SparkError {
	status := status.Convert(e)
	// If there was no error, simply pass through.
	if status == nil {
		return nil
	}
	result := &SparkError{
		Message: status.Message(),
		Code:    status.Code(),
		status:  status,
	}

	// Now lets, check if we can extract the error info from the details.
	for _, d := range status.Details() {
		switch info := d.(type) {
		case *errdetails.ErrorInfo:
			// Parse the parameters from the error details, but only parse them if
			// they're present.
			var params map[string]string
			if v, ok := info.GetMetadata()["messageParameters"]; ok {
				err := json.Unmarshal([]byte(v), &params)
				if err == nil {
					// The message parameters is properly formatted JSON, if for some reason
					// this is not the case, errors are ignored.
					result.Parameters = params
				}
			}
			result.SqlState = info.GetMetadata()["sqlState"]
			result.ErrorClass = info.GetMetadata()["errorClass"]
			result.ErrorId = info.GetMetadata()["errorId"]
			result.Reason = info.Reason
		}
	}
	return result
}
