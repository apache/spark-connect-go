package literal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestString_ToPBLiteral(t *testing.T) {
	s := String("test")
	assert.Equal(t, "test", s.ToPBLiteral().GetString_())
}

func TestNull_ToPBLiteral(t *testing.T) {
	n := Null(0)
	assert.NotNil(t, n.ToPBLiteral())
}

func TestBinary_ToPBLiteral(t *testing.T) {
	b := Binary([]byte("test"))
	assert.Equal(t, []byte("test"), b.ToPBLiteral().GetBinary())
}

func TestBoolean_ToPBLiteral(t *testing.T) {
	b := Boolean(true)
	assert.Equal(t, true, b.ToPBLiteral().GetBoolean())
}

func TestByte_ToPBLiteral(t *testing.T) {
	b := Byte(1)
	assert.Equal(t, int32(1), b.ToPBLiteral().GetByte())
}

func TestShort_ToPBLiteral(t *testing.T) {
	s := Short(1)
	assert.Equal(t, int32(1), s.ToPBLiteral().GetShort())
}

func TestInteger_ToPBLiteral(t *testing.T) {
	i := Integer(1)
	assert.Equal(t, int32(1), i.ToPBLiteral().GetInteger())
}

func TestLong_ToPBLiteral(t *testing.T) {
	l := Long(1)
	assert.Equal(t, int64(1), l.ToPBLiteral().GetLong())
}

func TestFloat_ToPBLiteral(t *testing.T) {
	f := Float(1.0)
	assert.Equal(t, float32(1.0), f.ToPBLiteral().GetFloat())
}

func TestDouble_ToPBLiteral(t *testing.T) {
	d := Double(1.0)
	assert.Equal(t, float64(1.0), d.ToPBLiteral().GetDouble())
}

func TestDecimal_ToPBLiteral(t *testing.T) {
	d := Decimal("1.0", 10, 0)
	assert.Equal(t, "1.0", d.ToPBLiteral().GetDecimal().GetValue())
	assert.Equal(t, int32(10), d.ToPBLiteral().GetDecimal().GetPrecision())
	assert.Equal(t, int32(0), d.ToPBLiteral().GetDecimal().GetScale())
}

func TestDate_ToPBLiteral(t *testing.T) {
	d := Date(1)
	assert.Equal(t, int32(1), d.ToPBLiteral().GetDate())
}

func TestTimestamp_ToPBLiteral(t *testing.T) {
	now := time.Now()
	stamp := Timestamp(now)
	assert.Equal(t, now.Unix(), stamp.ToPBLiteral().GetTimestamp())
}

func TestTimestampNtz_ToPBLiteral(t *testing.T) {
	now := time.Now()
	stamp := TimestampNtz(now)
	assert.Equal(t, now.Unix(), stamp.ToPBLiteral().GetTimestampNtz())
}

func TestCalendarInterval_ToPBLiteral(t *testing.T) {
	ci := CalendarInterval(1, 2, 3)
	assert.Equal(t, int32(1), ci.ToPBLiteral().GetCalendarInterval().GetMonths())
	assert.Equal(t, int32(2), ci.ToPBLiteral().GetCalendarInterval().GetDays())
	assert.Equal(t, int64(3), ci.ToPBLiteral().GetCalendarInterval().GetMicroseconds())
}

func TestYearMonthInterval_ToPBLiteral(t *testing.T) {
	ymi := YearMonthInterval(1)
	assert.Equal(t, int32(1), ymi.ToPBLiteral().GetYearMonthInterval())
}

func TestDayTimeInterval_ToPBLiteral(t *testing.T) {
	dti := DayTimeInterval(1)
	assert.Equal(t, int64(1), dti.ToPBLiteral().GetDayTimeInterval())
}
