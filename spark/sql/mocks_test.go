package sql

import (
	"context"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

type testExecutor struct {
	client   *ExecutePlanClient
	response *proto.AnalyzePlanResponse
	err      error
}

func (t *testExecutor) ExecutePlan(ctx context.Context, plan *proto.Plan) (*ExecutePlanClient, error) {
	if t.err != nil {
		return nil, t.err
	}
	return t.client, nil
}

func (t *testExecutor) AnalyzePlan(ctx context.Context, plan *proto.Plan) (*proto.AnalyzePlanResponse, error) {
	return t.response, nil
}
