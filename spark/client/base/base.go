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

package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/spark-connect-go/spark/sql/utils"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/internal/generated"
	"github.com/apache/spark-connect-go/spark/sql/types"
)

// ValidateTag enforces the Spark Connect tag rules: a tag must be non-empty
// and must not contain ','. See ExecutePlanRequest.tags in spark/connect/base.proto.
func ValidateTag(tag string) error {
	if tag == "" {
		return fmt.Errorf("spark connect tag cannot be an empty string")
	}
	if strings.ContainsRune(tag, ',') {
		return fmt.Errorf("spark connect tag cannot contain ',': %q", tag)
	}
	return nil
}

type SparkConnectRPCClient generated.SparkConnectServiceClient

// SparkConnectClient is the interface for executing a plan in Spark.
//
// This interface does not deal with the public Spark API abstractions but roughly deals on the
// RPC API level and the necessary translation of Arrow to Row objects.
type SparkConnectClient interface {
	ExecutePlan(ctx context.Context, plan *generated.Plan) (ExecuteResponseStream, error)
	ExecuteCommand(ctx context.Context, plan *generated.Plan) (arrow.Table, *types.StructType, map[string]any, error)
	AnalyzePlan(ctx context.Context, plan *generated.Plan) (*generated.AnalyzePlanResponse, error)
	Explain(ctx context.Context, plan *generated.Plan, explainMode utils.ExplainMode) (*generated.AnalyzePlanResponse, error)
	Persist(ctx context.Context, plan *generated.Plan, storageLevel utils.StorageLevel) error
	Unpersist(ctx context.Context, plan *generated.Plan) error
	GetStorageLevel(ctx context.Context, plan *generated.Plan) (*utils.StorageLevel, error)
	SparkVersion(ctx context.Context) (string, error)
	DDLParse(ctx context.Context, sql string) (*types.StructType, error)
	SameSemantics(ctx context.Context, plan1 *generated.Plan, plan2 *generated.Plan) (bool, error)
	SemanticHash(ctx context.Context, plan *generated.Plan) (int32, error)
	Config(ctx context.Context, configRequest *generated.ConfigRequest_Operation) (*generated.ConfigResponse, error)

	// AddTag attaches the given tag to every subsequent ExecutePlan request issued by this client.
	// Tags follow the Spark Connect contract enforced by ValidateTag: non-empty and no commas.
	AddTag(tag string) error
	// RemoveTag removes the given tag from this client. Removing a tag that was never added is a no-op.
	RemoveTag(tag string) error
	// GetTags returns a snapshot of the tags currently attached to this client.
	// The slice is sorted lexicographically so callers and tests get a deterministic order.
	GetTags() []string
	// ClearTags drops every tag attached to this client.
	ClearTags()
}

type ExecuteResponseStream interface {
	ToTable() (*types.StructType, arrow.Table, error)
	Properties() map[string]any
}
