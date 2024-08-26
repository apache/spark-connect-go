package literal

import (
	"time"

	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

type Value interface {
	ToPBLiteral() *proto.Expression_Literal
}

type String string

func (s String) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_String_{
			String_: string(s),
		},
	}
}

type Null byte

func (n Null) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Null{},
	}
}

type Binary []byte

func (b Binary) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Binary{
			Binary: b,
		},
	}
}

type Boolean bool

func (b Boolean) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Boolean{
			Boolean: bool(b),
		},
	}
}

type Byte byte

func (b Byte) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Byte{
			Byte: int32(b), // Why is the pb type defined to be int32?
		},
	}
}

type Short int16

func (s Short) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Short{
			Short: int32(s),
		},
	}
}

type Integer int32

func (i Integer) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Integer{
			Integer: int32(i),
		},
	}
}

type Long int64

func (l Long) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Long{
			Long: int64(l),
		},
	}
}

type Float float32

func (f Float) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Float{
			Float: float32(f),
		},
	}
}

type Double float64

func (d Double) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Double{
			Double: float64(d),
		},
	}
}

func Decimal(value string, precision int32, scale int32) Decimal_ {
	return Decimal_{
		Expression_Literal_Decimal: &proto.Expression_Literal_Decimal{
			Value:     value,
			Precision: &precision,
			Scale:     &scale,
		},
	}
}

type Decimal_ struct {
	*proto.Expression_Literal_Decimal
}

func (d *Decimal_) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Decimal_{
			Decimal: d.Expression_Literal_Decimal,
		},
	}
}

func NewDate(date time.Time) Date {
	inputUtc := date.UTC()
	epochUtc := time.Unix(0, 0).UTC()
	return Date(inputUtc.Sub(epochUtc).Hours() / 24) // Going with with description in the protobuf file
}

type Date int32

func (d Date) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Date{
			Date: int32(d),
		},
	}
}

type Timestamp time.Time

func (t Timestamp) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_Timestamp{
			Timestamp: time.Time(t).Unix(),
		},
	}
}

type TimestampNtz time.Time

func (t TimestampNtz) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_TimestampNtz{
			TimestampNtz: time.Time(t).Unix(),
		},
	}
}

func CalendarInterval(months int32, days int32, microseconds int64) CalendarInterval_ {
	return CalendarInterval_{
		Expression_Literal_CalendarInterval: &proto.Expression_Literal_CalendarInterval{
			Months:       months,
			Days:         days,
			Microseconds: microseconds,
		},
	}
}

type CalendarInterval_ struct {
	*proto.Expression_Literal_CalendarInterval
}

func (c CalendarInterval_) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_CalendarInterval_{
			CalendarInterval: c.Expression_Literal_CalendarInterval,
		},
	}
}

type YearMonthInterval int32

func (y YearMonthInterval) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_YearMonthInterval{
			YearMonthInterval: int32(y),
		},
	}
}

type DayTimeInterval int64

func (d DayTimeInterval) ToPBLiteral() *proto.Expression_Literal {
	return &proto.Expression_Literal{
		LiteralType: &proto.Expression_Literal_DayTimeInterval{
			DayTimeInterval: int64(d),
		},
	}
}

// TODO: Missing Array, Map, Struct. Need to think about the exact API for these.
