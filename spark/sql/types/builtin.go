//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"context"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

type LiteralType interface {
	ToProto(ctx context.Context) (*proto.Expression, error)
}

type NumericLiteral interface {
	LiteralType
	// marker method for compile time safety.
	isNumericLiteral()
}

type PrimitiveTypeLiteral interface {
	LiteralType
	isPrimitiveTypeLiteral()
}

type Int8 int8

func (t Int8) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Byte{Byte: int32(t)},
			},
		},
	}, nil
}

func (t Int8) isNumericLiteral() {}

func (t Int8) isPrimitiveTypeLiteral() {}

type Int16 int16

func (t Int16) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Short{Short: int32(t)},
			},
		},
	}, nil
}

func (t Int16) isNumericLiteral() {}

func (t Int16) isPrimitiveTypeLiteral() {}

type Int32 int32

func (t Int32) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Integer{Integer: int32(t)},
			},
		},
	}, nil
}

func (t Int32) isNumericLiteral() {}

func (t Int32) isPrimitiveTypeLiteral() {}

type Int64 int64

func (t Int64) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Long{Long: int64(t)},
			},
		},
	}, nil
}

func (t Int64) isNumericLiteral() {}

func (t Int64) isPrimitiveTypeLiteral() {}

type Int int

func (t Int) ToProto(ctx context.Context) (*proto.Expression, error) {
	return Int64(t).ToProto(ctx)
}

func (t Int) isNumericLiteral() {}

func (t Int) isPrimitiveTypeLiteral() {}

type Float32 float32

func (t Float32) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Float{Float: float32(t)},
			},
		},
	}, nil
}

func (t Float32) isNumericLiteral() {}

func (t Float32) isPrimitiveTypeLiteral() {}

type Float64 float64

func (t Float64) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Double{Double: float64(t)},
			},
		},
	}, nil
}

func (t Float64) isNumericLiteral() {}

func (t Float64) isPrimitiveTypeLiteral() {}

type String string

func (t String) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_String_{String_: string(t)},
			},
		},
	}, nil
}

func (t String) isPrimitiveTypeLiteral() {}

type Boolean bool

func (t Boolean) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Boolean{Boolean: bool(t)},
			},
		},
	}, nil
}

func (t Boolean) isPrimitiveTypeLiteral() {}

type Binary []byte

func (t Binary) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Binary{Binary: t},
			},
		},
	}, nil
}

type Int8Nil struct{}

func (t Int8Nil) isNumericLiteral() {}

func (t Int8Nil) isPrimitiveTypeLiteral() {}

func (t Int8Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Byte_{
							Byte: &proto.DataType_Byte{},
						},
					},
				},
			},
		},
	}, nil
}

type Int16Nil struct{}

func (t Int16Nil) isNumericLiteral() {}

func (t Int16Nil) isPrimitiveTypeLiteral() {}

func (t Int16Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Short_{
							Short: &proto.DataType_Short{},
						},
					},
				},
			},
		},
	}, nil
}

type Int32Nil struct{}

func (t Int32Nil) isNumericLiteral() {}

func (t Int32Nil) isPrimitiveTypeLiteral() {}

func (t Int32Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Integer_{
							Integer: &proto.DataType_Integer{},
						},
					},
				},
			},
		},
	}, nil
}

type Int64Nil struct{}

func (t Int64Nil) isNumericLiteral() {}

func (t Int64Nil) isPrimitiveTypeLiteral() {}

func (t Int64Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Long_{
							Long: &proto.DataType_Long{},
						},
					},
				},
			},
		},
	}, nil
}

type IntNil struct{}

func (t IntNil) isNumericLiteral() {}

func (t IntNil) isPrimitiveTypeLiteral() {}

func (t IntNil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return Int64Nil{}.ToProto(ctx)
}

type Float32Nil struct{}

func (t Float32Nil) isNumericLiteral() {}

func (t Float32Nil) isPrimitiveTypeLiteral() {}

func (t Float32Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Float_{
							Float: &proto.DataType_Float{},
						},
					},
				},
			},
		},
	}, nil
}

type Float64Nil struct{}

func (t Float64Nil) isNumericLiteral() {}

func (t Float64Nil) isPrimitiveTypeLiteral() {}

func (t Float64Nil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Double_{
							Double: &proto.DataType_Double{},
						},
					},
				},
			},
		},
	}, nil
}

type StringNil struct{}

func (t StringNil) isPrimitiveTypeLiteral() {}

func (t StringNil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_String_{
							String_: &proto.DataType_String{},
						},
					},
				},
			},
		},
	}, nil
}

type BooleanNil struct{}

func (t BooleanNil) isPrimitiveTypeLiteral() {}

func (t BooleanNil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Boolean_{
							Boolean: &proto.DataType_Boolean{},
						},
					},
				},
			},
		},
	}, nil
}

type BinaryNil struct{}

func (t BinaryNil) ToProto(ctx context.Context) (*proto.Expression, error) {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: &proto.Expression_Literal{
				LiteralType: &proto.Expression_Literal_Null{
					Null: &proto.DataType{
						Kind: &proto.DataType_Binary_{
							Binary: &proto.DataType_Binary{},
						},
					},
				},
			},
		},
	}, nil
}
