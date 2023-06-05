package sql

import proto "github.com/apache/spark-connect-go/v_3_4/internal/generated"

type DataFrameReader interface {
	Format(source string) DataFrameReader
	Load(path string) (DataFrame, error)
}

type dataFrameReaderImpl struct {
	sparkSession *sparkSessionImpl
	formatSource string
}

func (w *dataFrameReaderImpl) Format(source string) DataFrameReader {
	w.formatSource = source
	return w
}

func (w *dataFrameReaderImpl) Load(path string) (DataFrame, error) {
	var format *string
	if w.formatSource != "" {
		format = &w.formatSource
	}
	df := &dataFrameImpl{
		sparkSession: w.sparkSession,
		relation: &proto.Relation{
			RelType: &proto.Relation_Read{
				Read: &proto.Read{
					ReadType: &proto.Read_DataSource_{
						DataSource: &proto.Read_DataSource{
							Format: format,
							Paths:  []string{path},
						},
					},
				},
			},
		},
	}
	return df, nil
}
