package expression

import (
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
	"github.com/apache/spark-connect-go/v35/spark/sql/literal"
)

func Literal(value literal.Value) *proto.Expression {
	return &proto.Expression{
		ExprType: &proto.Expression_Literal_{
			Literal: value.ToPBLiteral(),
		},
	}
}

func Of(literals ...literal.Value) []*proto.Expression {
	expressions := make([]*proto.Expression, len(literals))
	for i, l := range literals {
		expressions[i] = Literal(l)
	}
	return expressions
}
