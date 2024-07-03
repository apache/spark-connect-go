package sql

import (
	"context"
	"testing"

	"github.com/apache/spark-connect-go/v1/client/sparkerrors"
	proto "github.com/apache/spark-connect-go/v1/internal/generated/spark/connect"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzePlanCallsAnalyzePlanOnClient(t *testing.T) {
	ctx := context.Background()
	reponse := &proto.AnalyzePlanResponse{}
	session := &sparkSessionImpl{
		client: &connectServiceClient{
			analysePlanResponse: reponse,
		},
	}
	resp, err := session.analyzePlan(ctx, &proto.Plan{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAnalyzePlanFailsIfClientFails(t *testing.T) {
	ctx := context.Background()
	session := &sparkSessionImpl{
		client: &connectServiceClient{
			err: assert.AnError,
		},
	}
	resp, err := session.analyzePlan(ctx, &proto.Plan{})
	assert.Nil(t, resp)
	assert.Error(t, err)
}

func TestExecutePlanCallsExecutePlanOnClient(t *testing.T) {
	ctx := context.Background()

	plan := &proto.Plan{}
	request := &proto.ExecutePlanRequest{
		Plan: plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	session := &sparkSessionImpl{
		client: &connectServiceClient{
			executePlanClient:          &executePlanClient{},
			expectedExecutePlanRequest: request,
			t:                          t,
		},
	}
	resp, err := session.executePlan(ctx, plan)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestSQLCallsExecutePlanWithSQLOnClient(t *testing.T) {
	ctx := context.Background()

	query := "select * from bla"
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_SqlCommand{
					SqlCommand: &proto.SqlCommand{
						Sql: query,
					},
				},
			},
		},
	}
	request := &proto.ExecutePlanRequest{
		Plan: plan,
		UserContext: &proto.UserContext{
			UserId: "na",
		},
	}
	session := &sparkSessionImpl{
		client: &connectServiceClient{
			executePlanClient: &executePlanClient{&protoClient{
				recvResponse: &proto.ExecutePlanResponse{
					ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
						SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{},
					},
				},
			}},
			expectedExecutePlanRequest: request,
			t:                          t,
		},
	}
	resp, err := session.Sql(ctx, query)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestNewSessionBuilderCreatesASession(t *testing.T) {
	ctx := context.Background()
	spark, err := NewSessionBuilder().Remote("sc:connection").Build(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, spark)
}

func TestNewSessionBuilderFailsIfConnectionStringIsInvalid(t *testing.T) {
	ctx := context.Background()
	spark, err := NewSessionBuilder().Remote("invalid").Build(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sparkerrors.InvalidInputError)
	assert.Nil(t, spark)
}
