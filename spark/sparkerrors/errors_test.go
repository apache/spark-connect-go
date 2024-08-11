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

package sparkerrors

import (
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/assert"
)

func TestWithTypeGivesAndErrorThatIsOfThatType(t *testing.T) {
	err := WithType(assert.AnError, ConnectionError)
	assert.ErrorIs(t, err, ConnectionError)
}

func TestErrorStringContainsErrorType(t *testing.T) {
	err := WithType(assert.AnError, ConnectionError)
	assert.Contains(t, err.Error(), ConnectionError.Error())
}

func TestGRPCErrorConversion(t *testing.T) {
	err := status.Error(codes.Internal, "invalid argument")
	se := FromRPCError(err)
	assert.Equal(t, se.Code, codes.Internal)
	assert.Equal(t, se.Message, "invalid argument")
}

func TestNonGRPCErrorsAreConvertedAsWell(t *testing.T) {
	err := assert.AnError
	se := FromRPCError(err)
	assert.Equal(t, se.Code, codes.Unknown)
	assert.Equal(t, se.Message, assert.AnError.Error())
}

func TestErrorDetailsExtractionFromGRPCStatus(t *testing.T) {
	status := status.New(codes.Internal, "AnalysisException")
	status, _ = status.WithDetails(&errdetails.ErrorInfo{
		Reason:   "AnalysisException",
		Domain:   "spark.sql",
		Metadata: map[string]string{},
	})

	err := status.Err()
	se := FromRPCError(err)
	assert.Equal(t, codes.Internal, se.Code)
	assert.Equal(t, "AnalysisException", se.Message)
	assert.Equal(t, "AnalysisException", se.Reason)
}

func TestErrorDetailsWithSqlStateAndClass(t *testing.T) {
	status := status.New(codes.Internal, "AnalysisException")
	status, _ = status.WithDetails(&errdetails.ErrorInfo{
		Reason: "AnalysisException",
		Domain: "spark.sql",
		Metadata: map[string]string{
			"sqlState":          "42000",
			"errorClass":        "ERROR_CLASS",
			"errorId":           "errorId",
			"messageParameters": "",
		},
	})

	err := status.Err()
	se := FromRPCError(err)
	assert.Equal(t, codes.Internal, se.Code)
	assert.Equal(t, "AnalysisException", se.Message)
	assert.Equal(t, "AnalysisException", se.Reason)
	assert.Equal(t, "42000", se.SqlState)
	assert.Equal(t, "ERROR_CLASS", se.ErrorClass)
	assert.Equal(t, "errorId", se.ErrorId)
}

func TestErrorDetailsWithMessageParameterParsing(t *testing.T) {
	type param struct {
		TestName string
		Input    string
		Expected map[string]string
	}

	params := []param{
		{"empty input", "", nil},
		{"empty input", "{", nil},
		{"parse error", "{}", map[string]string{}},
		{"valid input", "{\"key\":\"value\"}", map[string]string{"key": "value"}},
	}

	for _, p := range params {
		t.Run(p.TestName, func(t *testing.T) {
			status := status.New(codes.Internal, "AnalysisException")
			status, _ = status.WithDetails(&errdetails.ErrorInfo{
				Reason: "AnalysisException",
				Domain: "spark.sql",
				Metadata: map[string]string{
					"sqlState":          "42000",
					"errorClass":        "ERROR_CLASS",
					"errorId":           "errorId",
					"messageParameters": p.Input,
				},
			})

			err := status.Err()
			se := FromRPCError(err)
			assert.Equal(t, codes.Internal, se.Code)
			assert.Equal(t, "AnalysisException", se.Message)
			assert.Equal(t, "AnalysisException", se.Reason)
			assert.Equal(t, "42000", se.SqlState)
			assert.Equal(t, "ERROR_CLASS", se.ErrorClass)
			assert.Equal(t, "errorId", se.ErrorId)
			assert.Equal(t, p.Expected, se.Parameters)
		})
	}
}

func TestSparkError_Error(t *testing.T) {
	type fields struct {
		SqlState   string
		ErrorClass string
		Reason     string
		Message    string
		Code       codes.Code
		ErrorId    string
		Parameters map[string]string
		status     *status.Status
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"UNKNOWN",
			fields{
				Code:    codes.Unknown,
				Message: "Unknown error",
			},
			"[Unknown] Unknown error",
		},
		{
			"Analysis Exception",
			fields{
				SqlState:   "42703",
				ErrorClass: "UNRESOLVED_COLUMN.WITH_SUGGESTION",
				Message:    "A column, variable, or function parameter with name `id2` cannot be resolved. Did you mean one of the following? [`id`]",
				Code:       codes.Internal,
			},
			"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `id2` cannot be resolved. Did you mean one of the following? [`id`]. SQLSTATE: 42703",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := SparkError{
				SqlState:   tt.fields.SqlState,
				ErrorClass: tt.fields.ErrorClass,
				Reason:     tt.fields.Reason,
				Message:    tt.fields.Message,
				Code:       tt.fields.Code,
				ErrorId:    tt.fields.ErrorId,
				Parameters: tt.fields.Parameters,
				status:     tt.fields.status,
			}
			assert.Equalf(t, tt.want, e.Error(), "Error()")
		})
	}
}
