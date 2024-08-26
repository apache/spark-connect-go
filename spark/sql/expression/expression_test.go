package expression

import (
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql/literal"
	"github.com/stretchr/testify/assert"
)

func TestLiteral(t *testing.T) {
	l := Literal(literal.String("test"))
	assert.Equal(t, "test", l.GetLiteral().GetString_())
}

func TestOf(t *testing.T) {
	literals := []literal.Value{
		literal.String("test"),
		literal.Null(0),
		literal.Binary([]byte("test")),
		literal.Boolean(true),
		literal.Byte(1),
		literal.Short(1),
		literal.Integer(1),
		literal.Long(1),
		literal.Float(1.0),
		literal.Double(1.0),
	}

	expressions := Of(literals...)
	assert.Len(t, expressions, len(literals))
	for i, l := range literals {
		assert.Equal(t, l.ToPBLiteral(), expressions[i].GetLiteral())
	}
}
