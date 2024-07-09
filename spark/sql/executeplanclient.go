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

package sql

import (
	"errors"
	"fmt"
	"io"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

type ExecutePlanClient struct {
	proto.SparkConnectService_ExecutePlanClient
}

func NewExecutePlanClient(responseClient proto.SparkConnectService_ExecutePlanClient) *ExecutePlanClient {
	return &ExecutePlanClient{
		responseClient,
	}
}

// consumeAll reads through the returned GRPC stream from Spark Connect Driver. It will
// discard the returned data if there is no error. This is necessary for handling GRPC response for
// saving data frame, since such consuming will trigger Spark Connect Driver really saving data frame.
// If we do not consume the returned GRPC stream, Spark Connect Driver will not really save data frame.
func (c *ExecutePlanClient) consumeAll() error {
	for {
		_, err := c.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			} else {
				return sparkerrors.WithType(fmt.Errorf("failed to receive plan execution response: %w", err), sparkerrors.ReadError)
			}
		}
	}
}
