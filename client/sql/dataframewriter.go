package sql

import (
	"fmt"
	proto "github.com/apache/spark-connect-go/v_3_4/internal/generated"
	"strings"
)

type DataFrameWriter interface {
	Mode(saveMode string) DataFrameWriter
	Format(source string) DataFrameWriter
	Save(path string) error
}

type dataFrameWriterImpl struct {
	sparkSession *sparkSessionImpl
	relation     *proto.Relation
	saveMode     string
	formatSource string
}

func (w *dataFrameWriterImpl) Mode(saveMode string) DataFrameWriter {
	w.saveMode = saveMode
	return w
}

func (w *dataFrameWriterImpl) Format(source string) DataFrameWriter {
	w.formatSource = source
	return w
}

func (w *dataFrameWriterImpl) Save(path string) error {
	saveMode, err := getSaveMode(w.saveMode)
	if err != nil {
		return err
	}
	var source *string
	if w.formatSource != "" {
		source = &w.formatSource
	}
	plan := &proto.Plan{
		OpType: &proto.Plan_Command{
			Command: &proto.Command{
				CommandType: &proto.Command_WriteOperation{
					WriteOperation: &proto.WriteOperation{
						Input:  w.relation,
						Mode:   saveMode,
						Source: source,
						SaveType: &proto.WriteOperation_Path{
							Path: path,
						},
					},
				},
			},
		},
	}
	responseClient, err := w.sparkSession.executePlan(plan)
	if err != nil {
		return err
	}

	return consumeExecutePlanClient(responseClient)
}

func getSaveMode(mode string) (proto.WriteOperation_SaveMode, error) {
	if mode == "" {
		return proto.WriteOperation_SAVE_MODE_UNSPECIFIED, nil
	} else if strings.EqualFold(mode, "Append") {
		return proto.WriteOperation_SAVE_MODE_APPEND, nil
	} else if strings.EqualFold(mode, "Overwrite") {
		return proto.WriteOperation_SAVE_MODE_OVERWRITE, nil
	} else if strings.EqualFold(mode, "ErrorIfExists") {
		return proto.WriteOperation_SAVE_MODE_ERROR_IF_EXISTS, nil
	} else if strings.EqualFold(mode, "Ignore") {
		return proto.WriteOperation_SAVE_MODE_IGNORE, nil
	} else {
		return 0, fmt.Errorf("unsupported save mode: %s", mode)
	}
}
