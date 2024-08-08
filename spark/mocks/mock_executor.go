package mocks

import (
	"context"

	"github.com/apache/spark-connect-go/v35/spark/client/base"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
)

type TestExecutor struct {
	Client   base.ExecuteResponseStream
	response *generated.AnalyzePlanResponse
	Err      error
}

func (t *TestExecutor) ExecutePlan(ctx context.Context, plan *generated.Plan) (base.ExecuteResponseStream, error) {
	if t.Err != nil {
		return nil, t.Err
	}
	return t.Client, nil
}

func (t *TestExecutor) AnalyzePlan(ctx context.Context, plan *generated.Plan) (*generated.AnalyzePlanResponse, error) {
	return t.response, nil
}

func (t *TestExecutor) ExecuteCommand(ctx context.Context, plan *generated.Plan) (arrow.Table, *types.StructType, map[string]interface{}, error) {
	if t.Err != nil {
		return nil, nil, nil, t.Err
	}
	return nil, nil, nil, nil
}
