package session

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/mocks"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type connectServiceClient struct {
	t *testing.T

	analysePlanResponse        *proto.AnalyzePlanResponse
	executePlanClient          proto.SparkConnectService_ExecutePlanClient
	expectedExecutePlanRequest *proto.ExecutePlanRequest

	err error
}

func (c *connectServiceClient) ExecutePlan(ctx context.Context, in *proto.ExecutePlanRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ExecutePlanClient, error) {
	if c.expectedExecutePlanRequest != nil {
		assert.Equal(c.t, c.expectedExecutePlanRequest, in)
	}
	return c.executePlanClient, c.err
}

func (c *connectServiceClient) AnalyzePlan(ctx context.Context, in *proto.AnalyzePlanRequest, opts ...grpc.CallOption) (*proto.AnalyzePlanResponse, error) {
	return c.analysePlanResponse, c.err
}

func (c *connectServiceClient) Config(ctx context.Context, in *proto.ConfigRequest, opts ...grpc.CallOption) (*proto.ConfigResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) AddArtifacts(ctx context.Context, opts ...grpc.CallOption) (proto.SparkConnectService_AddArtifactsClient, error) {
	return nil, c.err
}

func (c *connectServiceClient) ArtifactStatus(ctx context.Context, in *proto.ArtifactStatusesRequest, opts ...grpc.CallOption) (*proto.ArtifactStatusesResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) Interrupt(ctx context.Context, in *proto.InterruptRequest, opts ...grpc.CallOption) (*proto.InterruptResponse, error) {
	return nil, c.err
}

func (c *connectServiceClient) ReattachExecute(ctx context.Context, in *proto.ReattachExecuteRequest, opts ...grpc.CallOption) (proto.SparkConnectService_ReattachExecuteClient, error) {
	return nil, c.err
}

func (c *connectServiceClient) ReleaseExecute(ctx context.Context, in *proto.ReleaseExecuteRequest, opts ...grpc.CallOption) (*proto.ReleaseExecuteResponse, error) {
	return nil, c.err
}

func TestAnalyzePlanCallsAnalyzePlanOnClient(t *testing.T) {
	ctx := context.Background()
	reponse := &proto.AnalyzePlanResponse{}
	session := &sparkSessionImpl{
		client: &connectServiceClient{
			analysePlanResponse: reponse,
		},
	}
	resp, err := session.AnalyzePlan(ctx, &proto.Plan{})
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
	resp, err := session.AnalyzePlan(ctx, &proto.Plan{})
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
			executePlanClient:          &sql.ExecutePlanClient{},
			expectedExecutePlanRequest: request,
			t:                          t,
		},
	}
	resp, err := session.ExecutePlan(ctx, plan)
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
			executePlanClient: &sql.ExecutePlanClient{&mocks.ProtoClient{
				RecvResponse: &proto.ExecutePlanResponse{
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

func TestWriteResultStreamsArrowResultToCollector(t *testing.T) {
	ctx := context.Background()

	arrowFields := []arrow.Field{
		{
			Name: "show_string",
			Type: &arrow.StringType{},
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer arrowWriter.Close()

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	recordBuilder.Field(0).(*array.StringBuilder).Append("str1a\nstr1b")
	recordBuilder.Field(0).(*array.StringBuilder).Append("str2")

	record := recordBuilder.NewRecord()
	defer record.Release()

	err := arrowWriter.Write(record)
	require.Nil(t, err)

	query := "select * from bla"

	session := &sparkSessionImpl{
		client: &connectServiceClient{
			executePlanClient: &sql.ExecutePlanClient{&mocks.ProtoClient{
				RecvResponses: []*proto.ExecutePlanResponse{
					{
						ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
							SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{},
						},
					},
					{
						ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
							ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
								RowCount: 1,
								Data:     buf.Bytes(),
							},
						},
					},
				}},
			},
			t: t,
		},
	}
	resp, err := session.Sql(ctx, query)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	writer, err := resp.Repartition(1, []string{"1"})
	assert.NoError(t, err)
	collector := &testCollector{}
	err = writer.WriteResult(ctx, collector, 1, false)
	assert.NoError(t, err)
	assert.Equal(t, []any{"str2"}, collector.row)
}

type testCollector struct {
	row []any
}

func (t *testCollector) WriteRow(values []any) {
	t.row = values
}
