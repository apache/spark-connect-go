package sql

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"

	client2 "github.com/apache/spark-connect-go/v35/spark/client"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

type testExecutor struct {
	client   *client2.ExecutePlanClient
	response *proto.AnalyzePlanResponse
	err      error
}

func (t *testExecutor) ExecutePlan(ctx context.Context, plan *proto.Plan) (*client2.ExecutePlanClient, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.client, nil
}

func (t *testExecutor) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	return t.response, nil
}

func (t *testExecutor) ExecuteCommand(ctx context.Context, plan *proto.Plan) (arrow.Table, *types.StructType, map[string]interface{}, error) {
	if t.err != nil {
		return nil, nil, nil, t.err
	}
	return nil, nil, nil, nil
}
