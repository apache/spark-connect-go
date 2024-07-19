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
	"errors"
	"fmt"
)

type wrappedError struct {
	errorType error
	cause     error
}

func (w *wrappedError) Unwrap() []error {
	return []error{w.errorType, w.cause}
}

func (w *wrappedError) Error() string {
	return fmt.Sprintf("%s: %s", w.errorType, w.cause)
}

// WithType wraps an error with a type that can later be checked using `errors.Is`
func WithType(err error, errType errorType) error {
	return &wrappedError{cause: err, errorType: errType}
}

type errorType error

var (
	ConnectionError   = errorType(errors.New("connection error"))
	ReadError         = errorType(errors.New("read error"))
	ExecutionError    = errorType(errors.New("execution error"))
	InvalidInputError = errorType(errors.New("invalid input"))
	InvalidPlanError  = errorType(errors.New("invalid plan"))
)

type UnsupportedResponseTypeError struct {
	ResponseType interface{}
}

func (e UnsupportedResponseTypeError) Error() string {
	return fmt.Sprintf("Received unsupported response type: %T", e.ResponseType)
}

type InvalidServerSideSessionError struct {
	OwnSessionId      string
	ReceivedSessionId string
}

func (e InvalidServerSideSessionError) Error() string {
	return fmt.Sprintf("Received invalid session id %s, expected %s", e.ReceivedSessionId, e.OwnSessionId)
}
