package sql

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/spark-connect-go/v35/client/sparkerrors"
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
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
