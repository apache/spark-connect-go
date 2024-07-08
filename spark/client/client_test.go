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

package client_test

//func TestAnalyzePlanCallsAnalyzePlanOnClient(t *testing.T) {
//	ctx := context.Background()
//	response := &proto.AnalyzePlanResponse{}
//	c := client.NewSparkExecutorFromClient(testutils.NewConnectServiceClientMock(nil, nil, response, nil, nil), nil, "")
//	resp, err := c.AnalyzePlan(ctx, &proto.Plan{})
//	assert.NoError(t, err)
//	assert.NotNil(t, resp)
//}
//
//func TestAnalyzePlanFailsIfClientFails(t *testing.T) {
//	ctx := context.Background()
//	c := client.NewSparkExecutorFromClient(testutils.NewConnectServiceClientMock(nil, nil, nil, assert.AnError, nil), nil, "")
//	resp, err := c.AnalyzePlan(ctx, &proto.Plan{})
//	assert.Nil(t, resp)
//	assert.Error(t, err)
//}
//
//func TestExecutePlanCallsExecutePlanOnClient(t *testing.T) {
//	ctx := context.Background()
//	plan := &proto.Plan{}
//	request := &proto.ExecutePlanRequest{
//		Plan: plan,
//		UserContext: &proto.UserContext{
//			UserId: "na",
//		},
//	}
//	c := client.NewSparkExecutorFromClient(testutils.NewConnectServiceClientMock(request, &client.ExecutePlanClient{}, nil, nil, t), nil, "")
//	resp, err := c.ExecutePlan(ctx, plan)
//	assert.NoError(t, err)
//	assert.NotNil(t, resp)
//}
