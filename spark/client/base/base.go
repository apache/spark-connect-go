package base

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

type SparkConnectRPCClient generated.SparkConnectServiceClient

// SparkConnectClient is the interface for executing a plan in Spark.
//
// This interface does not deal with the public Spark API abstractions but roughly deals on the
// RPC API level and the necessary translation of Arrow to Row objects.
type SparkConnectClient interface {
	ExecutePlan(ctx context.Context, plan *generated.Plan) (ExecuteResponseStream, error)
	ExecuteCommand(ctx context.Context, plan *generated.Plan) (arrow.Table, *types.StructType, map[string]any, error)
	AnalyzePlan(ctx context.Context, plan *generated.Plan) (*generated.AnalyzePlanResponse, error)
}

type ExecuteResponseStream interface {
	ToTable() (*types.StructType, arrow.Table, error)
	Properties() map[string]any
}
