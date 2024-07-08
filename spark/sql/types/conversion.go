package types

import (
	"errors"

	"github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

func ConvertProtoDataTypeToStructType(input *generated.DataType) (*StructType, error) {
	dataTypeStruct := input.GetStruct()
	if dataTypeStruct == nil {
		return nil, sparkerrors.WithType(errors.New("dataType.GetStruct() is nil"), sparkerrors.InvalidInputError)
	}
	return &StructType{
		Fields: ConvertProtoStructFields(dataTypeStruct.Fields),
	}, nil
}

func ConvertProtoStructFields(input []*generated.DataType_StructField) []StructField {
	result := make([]StructField, len(input))
	for i, f := range input {
		result[i] = ConvertProtoStructField(f)
	}
	return result
}

func ConvertProtoStructField(field *generated.DataType_StructField) StructField {
	return StructField{
		Name:     field.Name,
		DataType: ConvertProtoDataTypeToDataType(field.DataType),
	}
}

// ConvertProtoDataTypeToDataType converts protobuf data type to Spark connect sql data type
func ConvertProtoDataTypeToDataType(input *generated.DataType) DataType {
	switch v := input.GetKind().(type) {
	case *generated.DataType_Boolean_:
		return BooleanType{}
	case *generated.DataType_Byte_:
		return ByteType{}
	case *generated.DataType_Short_:
		return ShortType{}
	case *generated.DataType_Integer_:
		return IntegerType{}
	case *generated.DataType_Long_:
		return LongType{}
	case *generated.DataType_Float_:
		return FloatType{}
	case *generated.DataType_Double_:
		return DoubleType{}
	case *generated.DataType_Decimal_:
		return DecimalType{}
	case *generated.DataType_String_:
		return StringType{}
	case *generated.DataType_Binary_:
		return BinaryType{}
	case *generated.DataType_Timestamp_:
		return TimestampType{}
	case *generated.DataType_TimestampNtz:
		return TimestampNtzType{}
	case *generated.DataType_Date_:
		return DateType{}
	default:
		return UnsupportedType{
			TypeInfo: v,
		}
	}
}
