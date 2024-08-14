// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package functions

import "github.com/apache/spark-connect-go/v35/spark/sql/column"

// BitwiseNOT is the Golang equivalent of bitwiseNOT: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitwiseNOT(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitwiseNOT", col))
}

// BitwiseNot is the Golang equivalent of bitwise_not: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitwiseNot(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitwise_not", col))
}

// BitCount is the Golang equivalent of bit_count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitCount(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_count", col))
}

// BitGet is the Golang equivalent of bit_get: (col: 'ColumnOrName', pos: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitGet(col column.Column, pos column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_get", col, pos))
}

// Getbit is the Golang equivalent of getbit: (col: 'ColumnOrName', pos: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Getbit(col column.Column, pos column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("getbit", col, pos))
}

// TODO: broadcast: (df: 'DataFrame') -> 'DataFrame'

// Coalesce is the Golang equivalent of coalesce: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Coalesce(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("coalesce", vals...))
}

// Greatest is the Golang equivalent of greatest: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Greatest(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("greatest", vals...))
}

// InputFileName is the Golang equivalent of input_file_name: () -> pyspark.sql.connect.column.Column
func InputFileName() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_name"))
}

// Least is the Golang equivalent of least: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Least(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("least", vals...))
}

// Isnan is the Golang equivalent of isnan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnan", col))
}

// Isnull is the Golang equivalent of isnull: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnull(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnull", col))
}

// MonotonicallyIncreasingId is the Golang equivalent of monotonically_increasing_id: () -> pyspark.sql.connect.column.Column
func MonotonicallyIncreasingId() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("monotonically_increasing_id"))
}

// Nanvl is the Golang equivalent of nanvl: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nanvl(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nanvl", col1, col2))
}

// Rand is the Golang equivalent of rand: (seed: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Rand(seed int64) column.Column {
	lit_seed := Lit(seed)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rand", lit_seed))
}

// Randn is the Golang equivalent of randn: (seed: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Randn(seed int64) column.Column {
	lit_seed := Lit(seed)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("randn", lit_seed))
}

// SparkPartitionId is the Golang equivalent of spark_partition_id: () -> pyspark.sql.connect.column.Column
func SparkPartitionId() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("spark_partition_id"))
}

// TODO: when: (condition: pyspark.sql.connect.column.Column, value: Any) -> pyspark.sql.connect.column.Column

// Asc is the Golang equivalent of asc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc", col))
}

// AscNullsFirst is the Golang equivalent of asc_nulls_first: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func AscNullsFirst(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc_nulls_first", col))
}

// AscNullsLast is the Golang equivalent of asc_nulls_last: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func AscNullsLast(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc_nulls_last", col))
}

// Desc is the Golang equivalent of desc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Desc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc", col))
}

// DescNullsFirst is the Golang equivalent of desc_nulls_first: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DescNullsFirst(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc_nulls_first", col))
}

// DescNullsLast is the Golang equivalent of desc_nulls_last: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DescNullsLast(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc_nulls_last", col))
}

// Abs is the Golang equivalent of abs: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Abs(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("abs", col))
}

// Acos is the Golang equivalent of acos: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Acos(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("acos", col))
}

// Acosh is the Golang equivalent of acosh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Acosh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("acosh", col))
}

// Asin is the Golang equivalent of asin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asin", col))
}

// Asinh is the Golang equivalent of asinh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asinh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asinh", col))
}

// Atan is the Golang equivalent of atan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Atan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atan", col))
}

// Atan2 is the Golang equivalent of atan2: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Atan2(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atan2", col1, col2))
}

// Atanh is the Golang equivalent of atanh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Atanh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atanh", col))
}

// Bin is the Golang equivalent of bin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Bin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bin", col))
}

// Bround is the Golang equivalent of bround: (col: 'ColumnOrName', scale: int = 0) -> pyspark.sql.connect.column.Column
func Bround(col column.Column, scale int64) column.Column {
	lit_scale := Lit(scale)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bround", col, lit_scale))
}

// Cbrt is the Golang equivalent of cbrt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cbrt(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cbrt", col))
}

// Ceil is the Golang equivalent of ceil: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ceil(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ceil", col))
}

// Ceiling is the Golang equivalent of ceiling: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ceiling(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ceiling", col))
}

// Conv is the Golang equivalent of conv: (col: 'ColumnOrName', fromBase: int, toBase: int) -> pyspark.sql.connect.column.Column
func Conv(col column.Column, fromBase int64, toBase int64) column.Column {
	lit_fromBase := Lit(fromBase)
	lit_toBase := Lit(toBase)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("conv", col, lit_fromBase, lit_toBase))
}

// Cos is the Golang equivalent of cos: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cos(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cos", col))
}

// Cosh is the Golang equivalent of cosh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cosh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cosh", col))
}

// Cot is the Golang equivalent of cot: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cot(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cot", col))
}

// Csc is the Golang equivalent of csc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Csc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("csc", col))
}

// Degrees is the Golang equivalent of degrees: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Degrees(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("degrees", col))
}

// E is the Golang equivalent of e: () -> pyspark.sql.connect.column.Column
func E() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("e"))
}

// Exp is the Golang equivalent of exp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Exp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("exp", col))
}

// Expm1 is the Golang equivalent of expm1: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Expm1(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("expm1", col))
}

// Factorial is the Golang equivalent of factorial: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Factorial(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("factorial", col))
}

// Floor is the Golang equivalent of floor: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Floor(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("floor", col))
}

// Hex is the Golang equivalent of hex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hex", col))
}

// Hypot is the Golang equivalent of hypot: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Hypot(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hypot", col1, col2))
}

// Log is the Golang equivalent of log: (arg1: Union[ForwardRef('ColumnOrName'), float], arg2: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Log(arg1 column.Column, arg2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log", arg1, arg2))
}

// Log10 is the Golang equivalent of log10: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log10(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log10", col))
}

// Log1p is the Golang equivalent of log1p: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log1p(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log1p", col))
}

// Ln is the Golang equivalent of ln: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ln(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ln", col))
}

// Log2 is the Golang equivalent of log2: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log2(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log2", col))
}

// Negative is the Golang equivalent of negative: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Negative(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("negative", col))
}

// Negate is the Golang equivalent of negate: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Negate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("negate", col))
}

// Pi is the Golang equivalent of pi: () -> pyspark.sql.connect.column.Column
func Pi() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pi"))
}

// Positive is the Golang equivalent of positive: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Positive(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("positive", col))
}

// Pmod is the Golang equivalent of pmod: (dividend: Union[ForwardRef('ColumnOrName'), float], divisor: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Pmod(dividend column.Column, divisor column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pmod", dividend, divisor))
}

// WidthBucket is the Golang equivalent of width_bucket: (v: 'ColumnOrName', min: 'ColumnOrName', max: 'ColumnOrName', numBucket: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func WidthBucket(v column.Column, min column.Column, max column.Column, numBucket column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("width_bucket", v, min, max, numBucket))
}

// Pow is the Golang equivalent of pow: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Pow(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pow", col1, col2))
}

// Radians is the Golang equivalent of radians: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Radians(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("radians", col))
}

// Rint is the Golang equivalent of rint: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rint(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rint", col))
}

// Round is the Golang equivalent of round: (col: 'ColumnOrName', scale: int = 0) -> pyspark.sql.connect.column.Column
func Round(col column.Column, scale int64) column.Column {
	lit_scale := Lit(scale)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("round", col, lit_scale))
}

// Sec is the Golang equivalent of sec: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sec(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sec", col))
}

// ShiftLeft is the Golang equivalent of shiftLeft: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftLeft(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftLeft", col, lit_numBits))
}

// Shiftleft is the Golang equivalent of shiftleft: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftleft(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftleft", col, lit_numBits))
}

// ShiftRight is the Golang equivalent of shiftRight: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftRight(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftRight", col, lit_numBits))
}

// Shiftright is the Golang equivalent of shiftright: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftright(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftright", col, lit_numBits))
}

// ShiftRightUnsigned is the Golang equivalent of shiftRightUnsigned: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftRightUnsigned(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftRightUnsigned", col, lit_numBits))
}

// Shiftrightunsigned is the Golang equivalent of shiftrightunsigned: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftrightunsigned(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftrightunsigned", col, lit_numBits))
}

// Signum is the Golang equivalent of signum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Signum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("signum", col))
}

// Sign is the Golang equivalent of sign: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sign(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sign", col))
}

// Sin is the Golang equivalent of sin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sin", col))
}

// Sinh is the Golang equivalent of sinh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sinh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sinh", col))
}

// Sqrt is the Golang equivalent of sqrt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sqrt(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sqrt", col))
}

// TryAdd is the Golang equivalent of try_add: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryAdd(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_add", left, right))
}

// TryAvg is the Golang equivalent of try_avg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryAvg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_avg", col))
}

// TryDivide is the Golang equivalent of try_divide: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryDivide(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_divide", left, right))
}

// TryMultiply is the Golang equivalent of try_multiply: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryMultiply(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_multiply", left, right))
}

// TrySubtract is the Golang equivalent of try_subtract: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TrySubtract(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_subtract", left, right))
}

// TrySum is the Golang equivalent of try_sum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TrySum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_sum", col))
}

// Tan is the Golang equivalent of tan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Tan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("tan", col))
}

// Tanh is the Golang equivalent of tanh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Tanh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("tanh", col))
}

// ToDegrees is the Golang equivalent of toDegrees: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToDegrees(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("toDegrees", col))
}

// ToRadians is the Golang equivalent of toRadians: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToRadians(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("toRadians", col))
}

// Unhex is the Golang equivalent of unhex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Unhex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unhex", col))
}

// ApproxCountDistinct is the Golang equivalent of approx_count_distinct: (col: 'ColumnOrName', rsd: Optional[float] = None) -> pyspark.sql.connect.column.Column
func ApproxCountDistinct(col column.Column, rsd float64) column.Column {
	lit_rsd := Lit(rsd)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("approx_count_distinct", col, lit_rsd))
}

// Avg is the Golang equivalent of avg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Avg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("avg", col))
}

// CollectList is the Golang equivalent of collect_list: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CollectList(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("collect_list", col))
}

// ArrayAgg is the Golang equivalent of array_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_agg", col))
}

// CollectSet is the Golang equivalent of collect_set: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CollectSet(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("collect_set", col))
}

// Corr is the Golang equivalent of corr: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Corr(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("corr", col1, col2))
}

// Count is the Golang equivalent of count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Count(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count", col))
}

// CountDistinct is the Golang equivalent of count_distinct: (col: 'ColumnOrName', *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountDistinct(col column.Column, cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, col)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_distinct", vals...))
}

// CovarPop is the Golang equivalent of covar_pop: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CovarPop(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("covar_pop", col1, col2))
}

// CovarSamp is the Golang equivalent of covar_samp: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CovarSamp(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("covar_samp", col1, col2))
}

// TODO: first: (col: 'ColumnOrName', ignorenulls: bool = False) -> pyspark.sql.connect.column.Column

// Grouping is the Golang equivalent of grouping: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Grouping(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("grouping", col))
}

// GroupingId is the Golang equivalent of grouping_id: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func GroupingId(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("grouping_id", vals...))
}

// CountMinSketch is the Golang equivalent of count_min_sketch: (col: 'ColumnOrName', eps: 'ColumnOrName', confidence: 'ColumnOrName', seed: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountMinSketch(col column.Column, eps column.Column, confidence column.Column, seed column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_min_sketch", col, eps, confidence, seed))
}

// Kurtosis is the Golang equivalent of kurtosis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Kurtosis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("kurtosis", col))
}

// TODO: last: (col: 'ColumnOrName', ignorenulls: bool = False) -> pyspark.sql.connect.column.Column

// Max is the Golang equivalent of max: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Max(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("max", col))
}

// MaxBy is the Golang equivalent of max_by: (col: 'ColumnOrName', ord: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MaxBy(col column.Column, ord column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("max_by", col, ord))
}

// Mean is the Golang equivalent of mean: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Mean(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mean", col))
}

// Median is the Golang equivalent of median: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Median(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("median", col))
}

// Min is the Golang equivalent of min: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Min(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("min", col))
}

// MinBy is the Golang equivalent of min_by: (col: 'ColumnOrName', ord: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MinBy(col column.Column, ord column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("min_by", col, ord))
}

// Mode is the Golang equivalent of mode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Mode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mode", col))
}

// TODO: percentile: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], frequency: Union[pyspark.sql.connect.column.Column, int] = 1) -> pyspark.sql.connect.column.Column

// TODO: percentile_approx: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], accuracy: Union[pyspark.sql.connect.column.Column, float] = 10000) -> pyspark.sql.connect.column.Column

// TODO: approx_percentile: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], accuracy: Union[pyspark.sql.connect.column.Column, float] = 10000) -> pyspark.sql.connect.column.Column

// Product is the Golang equivalent of product: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Product(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("product", col))
}

// Skewness is the Golang equivalent of skewness: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Skewness(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("skewness", col))
}

// Stddev is the Golang equivalent of stddev: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Stddev(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev", col))
}

// Std is the Golang equivalent of std: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Std(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("std", col))
}

// StddevSamp is the Golang equivalent of stddev_samp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func StddevSamp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev_samp", col))
}

// StddevPop is the Golang equivalent of stddev_pop: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func StddevPop(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev_pop", col))
}

// Sum is the Golang equivalent of sum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sum", col))
}

// SumDistinct is the Golang equivalent of sum_distinct: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func SumDistinct(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sum_distinct", col))
}

// VarPop is the Golang equivalent of var_pop: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func VarPop(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("var_pop", col))
}

// RegrAvgx is the Golang equivalent of regr_avgx: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrAvgx(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_avgx", y, x))
}

// RegrAvgy is the Golang equivalent of regr_avgy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrAvgy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_avgy", y, x))
}

// RegrCount is the Golang equivalent of regr_count: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrCount(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_count", y, x))
}

// RegrIntercept is the Golang equivalent of regr_intercept: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrIntercept(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_intercept", y, x))
}

// RegrR2 is the Golang equivalent of regr_r2: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrR2(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_r2", y, x))
}

// RegrSlope is the Golang equivalent of regr_slope: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSlope(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_slope", y, x))
}

// RegrSxx is the Golang equivalent of regr_sxx: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSxx(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_sxx", y, x))
}

// RegrSxy is the Golang equivalent of regr_sxy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSxy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_sxy", y, x))
}

// RegrSyy is the Golang equivalent of regr_syy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSyy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_syy", y, x))
}

// VarSamp is the Golang equivalent of var_samp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func VarSamp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("var_samp", col))
}

// Variance is the Golang equivalent of variance: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Variance(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("variance", col))
}

// Every is the Golang equivalent of every: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Every(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("every", col))
}

// BoolAnd is the Golang equivalent of bool_and: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BoolAnd(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bool_and", col))
}

// Some is the Golang equivalent of some: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Some(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("some", col))
}

// BoolOr is the Golang equivalent of bool_or: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BoolOr(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bool_or", col))
}

// BitAnd is the Golang equivalent of bit_and: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitAnd(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_and", col))
}

// BitOr is the Golang equivalent of bit_or: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitOr(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_or", col))
}

// BitXor is the Golang equivalent of bit_xor: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitXor(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_xor", col))
}

// CumeDist is the Golang equivalent of cume_dist: () -> pyspark.sql.connect.column.Column
func CumeDist() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cume_dist"))
}

// DenseRank is the Golang equivalent of dense_rank: () -> pyspark.sql.connect.column.Column
func DenseRank() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dense_rank"))
}

// TODO: lag: (col: 'ColumnOrName', offset: int = 1, default: Optional[Any] = None) -> pyspark.sql.connect.column.Column

// TODO: lead: (col: 'ColumnOrName', offset: int = 1, default: Optional[Any] = None) -> pyspark.sql.connect.column.Column

// TODO: nth_value: (col: 'ColumnOrName', offset: int, ignoreNulls: Optional[bool] = None) -> pyspark.sql.connect.column.Column

// TODO: any_value: (col: 'ColumnOrName', ignoreNulls: Union[bool, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: first_value: (col: 'ColumnOrName', ignoreNulls: Union[bool, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: last_value: (col: 'ColumnOrName', ignoreNulls: Union[bool, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// CountIf is the Golang equivalent of count_if: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountIf(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_if", col))
}

// HistogramNumeric is the Golang equivalent of histogram_numeric: (col: 'ColumnOrName', nBins: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func HistogramNumeric(col column.Column, nBins column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("histogram_numeric", col, nBins))
}

// Ntile is the Golang equivalent of ntile: (n: int) -> pyspark.sql.connect.column.Column
func Ntile(n int64) column.Column {
	lit_n := Lit(n)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ntile", lit_n))
}

// PercentRank is the Golang equivalent of percent_rank: () -> pyspark.sql.connect.column.Column
func PercentRank() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("percent_rank"))
}

// Rank is the Golang equivalent of rank: () -> pyspark.sql.connect.column.Column
func Rank() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rank"))
}

// RowNumber is the Golang equivalent of row_number: () -> pyspark.sql.connect.column.Column
func RowNumber() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("row_number"))
}

// TODO: aggregate: (col: 'ColumnOrName', initialValue: 'ColumnOrName', merge: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], finish: Optional[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// TODO: reduce: (col: 'ColumnOrName', initialValue: 'ColumnOrName', merge: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], finish: Optional[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// Array is the Golang equivalent of array: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func Array(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array", cols))
}

// TODO: array_append: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_contains: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// ArrayDistinct is the Golang equivalent of array_distinct: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayDistinct(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_distinct", col))
}

// ArrayExcept is the Golang equivalent of array_except: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayExcept(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_except", col1, col2))
}

// TODO: array_insert: (arr: 'ColumnOrName', pos: Union[ForwardRef('ColumnOrName'), int], value: Any) -> pyspark.sql.connect.column.Column

// ArrayIntersect is the Golang equivalent of array_intersect: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayIntersect(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_intersect", col1, col2))
}

// ArrayCompact is the Golang equivalent of array_compact: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayCompact(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_compact", col))
}

// ArrayJoin is the Golang equivalent of array_join: (col: 'ColumnOrName', delimiter: str, null_replacement: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ArrayJoin(col column.Column, delimiter string, null_replacement string) column.Column {
	lit_delimiter := Lit(delimiter)
	lit_null_replacement := Lit(null_replacement)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_join", col, lit_delimiter, lit_null_replacement))
}

// ArrayMax is the Golang equivalent of array_max: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayMax(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_max", col))
}

// ArrayMin is the Golang equivalent of array_min: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayMin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_min", col))
}

// ArraySize is the Golang equivalent of array_size: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraySize(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_size", col))
}

// Cardinality is the Golang equivalent of cardinality: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cardinality(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cardinality", col))
}

// TODO: array_position: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_prepend: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_remove: (col: 'ColumnOrName', element: Any) -> pyspark.sql.connect.column.Column

// ArrayRepeat is the Golang equivalent of array_repeat: (col: 'ColumnOrName', count: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func ArrayRepeat(col column.Column, count column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_repeat", col, count))
}

// TODO: array_sort: (col: 'ColumnOrName', comparator: Optional[Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// ArrayUnion is the Golang equivalent of array_union: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayUnion(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_union", col1, col2))
}

// ArraysOverlap is the Golang equivalent of arrays_overlap: (a1: 'ColumnOrName', a2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraysOverlap(a1 column.Column, a2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("arrays_overlap", a1, a2))
}

// ArraysZip is the Golang equivalent of arrays_zip: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraysZip(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("arrays_zip", vals...))
}

// Concat is the Golang equivalent of concat: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Concat(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("concat", vals...))
}

// CreateMap is the Golang equivalent of create_map: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func CreateMap(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("create_map", cols))
}

// TODO: element_at: (col: 'ColumnOrName', extraction: Any) -> pyspark.sql.connect.column.Column

// TryElementAt is the Golang equivalent of try_element_at: (col: 'ColumnOrName', extraction: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryElementAt(col column.Column, extraction column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_element_at", col, extraction))
}

// TODO: exists: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// Explode is the Golang equivalent of explode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Explode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("explode", col))
}

// ExplodeOuter is the Golang equivalent of explode_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ExplodeOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("explode_outer", col))
}

// TODO: filter: (col: 'ColumnOrName', f: Union[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]]) -> pyspark.sql.connect.column.Column

// Flatten is the Golang equivalent of flatten: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Flatten(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("flatten", col))
}

// TODO: forall: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// TODO: from_csv: (col: 'ColumnOrName', schema: Union[pyspark.sql.connect.column.Column, str], options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: from_json: (col: 'ColumnOrName', schema: Union[pyspark.sql.types.ArrayType, pyspark.sql.types.StructType, pyspark.sql.connect.column.Column, str], options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// Get is the Golang equivalent of get: (col: 'ColumnOrName', index: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Get(col column.Column, index column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("get", col, index))
}

// GetJsonObject is the Golang equivalent of get_json_object: (col: 'ColumnOrName', path: str) -> pyspark.sql.connect.column.Column
func GetJsonObject(col column.Column, path string) column.Column {
	lit_path := Lit(path)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("get_json_object", col, lit_path))
}

// JsonArrayLength is the Golang equivalent of json_array_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JsonArrayLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_array_length", col))
}

// JsonObjectKeys is the Golang equivalent of json_object_keys: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JsonObjectKeys(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_object_keys", col))
}

// Inline is the Golang equivalent of inline: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Inline(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("inline", col))
}

// InlineOuter is the Golang equivalent of inline_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func InlineOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("inline_outer", col))
}

// JsonTuple is the Golang equivalent of json_tuple: (col: 'ColumnOrName', *fields: str) -> pyspark.sql.connect.column.Column
func JsonTuple(col column.Column, fields string) column.Column {
	lit_fields := Lit(fields)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_tuple", col, lit_fields))
}

// MapConcat is the Golang equivalent of map_concat: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func MapConcat(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_concat", cols))
}

// TODO: map_contains_key: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// MapEntries is the Golang equivalent of map_entries: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapEntries(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_entries", col))
}

// TODO: map_filter: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// MapFromArrays is the Golang equivalent of map_from_arrays: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapFromArrays(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_from_arrays", col1, col2))
}

// MapFromEntries is the Golang equivalent of map_from_entries: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapFromEntries(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_from_entries", col))
}

// MapKeys is the Golang equivalent of map_keys: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapKeys(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_keys", col))
}

// MapValues is the Golang equivalent of map_values: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapValues(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_values", col))
}

// TODO: map_zip_with: (col1: 'ColumnOrName', col2: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// StrToMap is the Golang equivalent of str_to_map: (text: 'ColumnOrName', pairDelim: Optional[ForwardRef('ColumnOrName')] = None, keyValueDelim: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func StrToMap(text column.Column, pairDelim column.Column, keyValueDelim column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("str_to_map", text, pairDelim, keyValueDelim))
}

// Posexplode is the Golang equivalent of posexplode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Posexplode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("posexplode", col))
}

// PosexplodeOuter is the Golang equivalent of posexplode_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func PosexplodeOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("posexplode_outer", col))
}

// Reverse is the Golang equivalent of reverse: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Reverse(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("reverse", col))
}

// Sequence is the Golang equivalent of sequence: (start: 'ColumnOrName', stop: 'ColumnOrName', step: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Sequence(start column.Column, stop column.Column, step column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sequence", start, stop, step))
}

// TODO: schema_of_csv: (csv: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: schema_of_json: (json: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// Shuffle is the Golang equivalent of shuffle: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Shuffle(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shuffle", col))
}

// Size is the Golang equivalent of size: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Size(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("size", col))
}

// Slice is the Golang equivalent of slice: (col: 'ColumnOrName', start: Union[ForwardRef('ColumnOrName'), int], length: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Slice(col column.Column, start column.Column, length column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("slice", col, start, length))
}

// TODO: sort_array: (col: 'ColumnOrName', asc: bool = True) -> pyspark.sql.connect.column.Column

// Struct is the Golang equivalent of struct: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func Struct(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("struct", cols))
}

// NamedStruct is the Golang equivalent of named_struct: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func NamedStruct(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("named_struct", vals...))
}

// TODO: to_csv: (col: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: to_json: (col: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: transform: (col: 'ColumnOrName', f: Union[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]]) -> pyspark.sql.connect.column.Column

// TODO: transform_keys: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// TODO: transform_values: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// TODO: zip_with: (left: 'ColumnOrName', right: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// Upper is the Golang equivalent of upper: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Upper(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("upper", col))
}

// Lower is the Golang equivalent of lower: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Lower(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lower", col))
}

// Ascii is the Golang equivalent of ascii: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ascii(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ascii", col))
}

// Base64 is the Golang equivalent of base64: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Base64(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("base64", col))
}

// Unbase64 is the Golang equivalent of unbase64: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Unbase64(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unbase64", col))
}

// Ltrim is the Golang equivalent of ltrim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ltrim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ltrim", col))
}

// Rtrim is the Golang equivalent of rtrim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rtrim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rtrim", col))
}

// Trim is the Golang equivalent of trim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Trim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("trim", col))
}

// ConcatWs is the Golang equivalent of concat_ws: (sep: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ConcatWs(sep string, cols ...column.Column) column.Column {
	lit_sep := Lit(sep)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_sep)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("concat_ws", vals...))
}

// Decode is the Golang equivalent of decode: (col: 'ColumnOrName', charset: str) -> pyspark.sql.connect.column.Column
func Decode(col column.Column, charset string) column.Column {
	lit_charset := Lit(charset)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("decode", col, lit_charset))
}

// Encode is the Golang equivalent of encode: (col: 'ColumnOrName', charset: str) -> pyspark.sql.connect.column.Column
func Encode(col column.Column, charset string) column.Column {
	lit_charset := Lit(charset)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("encode", col, lit_charset))
}

// FormatNumber is the Golang equivalent of format_number: (col: 'ColumnOrName', d: int) -> pyspark.sql.connect.column.Column
func FormatNumber(col column.Column, d int64) column.Column {
	lit_d := Lit(d)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("format_number", col, lit_d))
}

// FormatString is the Golang equivalent of format_string: (format: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FormatString(format string, cols ...column.Column) column.Column {
	lit_format := Lit(format)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_format)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("format_string", vals...))
}

// Instr is the Golang equivalent of instr: (str: 'ColumnOrName', substr: str) -> pyspark.sql.connect.column.Column
func Instr(str column.Column, substr string) column.Column {
	lit_substr := Lit(substr)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("instr", str, lit_substr))
}

// Overlay is the Golang equivalent of overlay: (src: 'ColumnOrName', replace: 'ColumnOrName', pos: Union[ForwardRef('ColumnOrName'), int], len: Union[ForwardRef('ColumnOrName'), int] = -1) -> pyspark.sql.connect.column.Column
func Overlay(src column.Column, replace column.Column, pos column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("overlay", src, replace, pos, len))
}

// Sentences is the Golang equivalent of sentences: (string: 'ColumnOrName', language: Optional[ForwardRef('ColumnOrName')] = None, country: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Sentences(string column.Column, language column.Column, country column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sentences", string, language, country))
}

// Substring is the Golang equivalent of substring: (str: 'ColumnOrName', pos: int, len: int) -> pyspark.sql.connect.column.Column
func Substring(str column.Column, pos int64, len int64) column.Column {
	lit_pos := Lit(pos)
	lit_len := Lit(len)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substring", str, lit_pos, lit_len))
}

// SubstringIndex is the Golang equivalent of substring_index: (str: 'ColumnOrName', delim: str, count: int) -> pyspark.sql.connect.column.Column
func SubstringIndex(str column.Column, delim string, count int64) column.Column {
	lit_delim := Lit(delim)
	lit_count := Lit(count)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substring_index", str, lit_delim, lit_count))
}

// Levenshtein is the Golang equivalent of levenshtein: (left: 'ColumnOrName', right: 'ColumnOrName', threshold: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Levenshtein(left column.Column, right column.Column, threshold int64) column.Column {
	lit_threshold := Lit(threshold)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("levenshtein", left, right, lit_threshold))
}

// Locate is the Golang equivalent of locate: (substr: str, str: 'ColumnOrName', pos: int = 1) -> pyspark.sql.connect.column.Column
func Locate(substr string, str column.Column, pos int64) column.Column {
	lit_substr := Lit(substr)
	lit_pos := Lit(pos)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("locate", lit_substr, str, lit_pos))
}

// Lpad is the Golang equivalent of lpad: (col: 'ColumnOrName', len: int, pad: str) -> pyspark.sql.connect.column.Column
func Lpad(col column.Column, len int64, pad string) column.Column {
	lit_len := Lit(len)
	lit_pad := Lit(pad)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lpad", col, lit_len, lit_pad))
}

// Rpad is the Golang equivalent of rpad: (col: 'ColumnOrName', len: int, pad: str) -> pyspark.sql.connect.column.Column
func Rpad(col column.Column, len int64, pad string) column.Column {
	lit_len := Lit(len)
	lit_pad := Lit(pad)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rpad", col, lit_len, lit_pad))
}

// Repeat is the Golang equivalent of repeat: (col: 'ColumnOrName', n: int) -> pyspark.sql.connect.column.Column
func Repeat(col column.Column, n int64) column.Column {
	lit_n := Lit(n)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("repeat", col, lit_n))
}

// Split is the Golang equivalent of split: (str: 'ColumnOrName', pattern: str, limit: int = -1) -> pyspark.sql.connect.column.Column
func Split(str column.Column, pattern string, limit int64) column.Column {
	lit_pattern := Lit(pattern)
	lit_limit := Lit(limit)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("split", str, lit_pattern, lit_limit))
}

// Rlike is the Golang equivalent of rlike: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rlike(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rlike", str, regexp))
}

// Regexp is the Golang equivalent of regexp: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Regexp(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp", str, regexp))
}

// RegexpLike is the Golang equivalent of regexp_like: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpLike(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_like", str, regexp))
}

// RegexpCount is the Golang equivalent of regexp_count: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpCount(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_count", str, regexp))
}

// RegexpExtract is the Golang equivalent of regexp_extract: (str: 'ColumnOrName', pattern: str, idx: int) -> pyspark.sql.connect.column.Column
func RegexpExtract(str column.Column, pattern string, idx int64) column.Column {
	lit_pattern := Lit(pattern)
	lit_idx := Lit(idx)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_extract", str, lit_pattern, lit_idx))
}

// TODO: regexp_extract_all: (str: 'ColumnOrName', regexp: 'ColumnOrName', idx: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: regexp_replace: (string: 'ColumnOrName', pattern: Union[str, pyspark.sql.connect.column.Column], replacement: Union[str, pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// RegexpSubstr is the Golang equivalent of regexp_substr: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpSubstr(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_substr", str, regexp))
}

// TODO: regexp_instr: (str: 'ColumnOrName', regexp: 'ColumnOrName', idx: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// Initcap is the Golang equivalent of initcap: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Initcap(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("initcap", col))
}

// Soundex is the Golang equivalent of soundex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Soundex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("soundex", col))
}

// Length is the Golang equivalent of length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Length(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("length", col))
}

// OctetLength is the Golang equivalent of octet_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func OctetLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("octet_length", col))
}

// BitLength is the Golang equivalent of bit_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_length", col))
}

// Translate is the Golang equivalent of translate: (srcCol: 'ColumnOrName', matching: str, replace: str) -> pyspark.sql.connect.column.Column
func Translate(srcCol column.Column, matching string, replace string) column.Column {
	lit_matching := Lit(matching)
	lit_replace := Lit(replace)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("translate", srcCol, lit_matching, lit_replace))
}

// ToBinary is the Golang equivalent of to_binary: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToBinary(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_binary", col, format))
}

// ToChar is the Golang equivalent of to_char: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToChar(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_char", col, format))
}

// ToVarchar is the Golang equivalent of to_varchar: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToVarchar(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_varchar", col, format))
}

// ToNumber is the Golang equivalent of to_number: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToNumber(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_number", col, format))
}

// Replace is the Golang equivalent of replace: (src: 'ColumnOrName', search: 'ColumnOrName', replace: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Replace(src column.Column, search column.Column, replace column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("replace", src, search, replace))
}

// SplitPart is the Golang equivalent of split_part: (src: 'ColumnOrName', delimiter: 'ColumnOrName', partNum: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func SplitPart(src column.Column, delimiter column.Column, partNum column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("split_part", src, delimiter, partNum))
}

// Substr is the Golang equivalent of substr: (str: 'ColumnOrName', pos: 'ColumnOrName', len: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Substr(str column.Column, pos column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substr", str, pos, len))
}

// ParseUrl is the Golang equivalent of parse_url: (url: 'ColumnOrName', partToExtract: 'ColumnOrName', key: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ParseUrl(url column.Column, partToExtract column.Column, key column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("parse_url", url, partToExtract, key))
}

// Printf is the Golang equivalent of printf: (format: 'ColumnOrName', *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Printf(format column.Column, cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, format)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("printf", vals...))
}

// UrlDecode is the Golang equivalent of url_decode: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UrlDecode(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("url_decode", str))
}

// UrlEncode is the Golang equivalent of url_encode: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UrlEncode(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("url_encode", str))
}

// Position is the Golang equivalent of position: (substr: 'ColumnOrName', str: 'ColumnOrName', start: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Position(substr column.Column, str column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("position", substr, str, start))
}

// Endswith is the Golang equivalent of endswith: (str: 'ColumnOrName', suffix: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Endswith(str column.Column, suffix column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("endswith", str, suffix))
}

// Startswith is the Golang equivalent of startswith: (str: 'ColumnOrName', prefix: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Startswith(str column.Column, prefix column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("startswith", str, prefix))
}

// Char is the Golang equivalent of char: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Char(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("char", col))
}

// TryToBinary is the Golang equivalent of try_to_binary: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryToBinary(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_binary", col, format))
}

// TryToNumber is the Golang equivalent of try_to_number: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryToNumber(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_number", col, format))
}

// Btrim is the Golang equivalent of btrim: (str: 'ColumnOrName', trim: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Btrim(str column.Column, trim column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("btrim", str, trim))
}

// CharLength is the Golang equivalent of char_length: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CharLength(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("char_length", str))
}

// CharacterLength is the Golang equivalent of character_length: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CharacterLength(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("character_length", str))
}

// Contains is the Golang equivalent of contains: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Contains(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("contains", left, right))
}

// Elt is the Golang equivalent of elt: (*inputs: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Elt(inputs ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, inputs...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("elt", vals...))
}

// FindInSet is the Golang equivalent of find_in_set: (str: 'ColumnOrName', str_array: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FindInSet(str column.Column, str_array column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("find_in_set", str, str_array))
}

// TODO: like: (str: 'ColumnOrName', pattern: 'ColumnOrName', escapeChar: Optional[ForwardRef('Column')] = None) -> pyspark.sql.connect.column.Column

// TODO: ilike: (str: 'ColumnOrName', pattern: 'ColumnOrName', escapeChar: Optional[ForwardRef('Column')] = None) -> pyspark.sql.connect.column.Column

// Lcase is the Golang equivalent of lcase: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Lcase(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lcase", str))
}

// Ucase is the Golang equivalent of ucase: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ucase(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ucase", str))
}

// Left is the Golang equivalent of left: (str: 'ColumnOrName', len: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Left(str column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("left", str, len))
}

// Right is the Golang equivalent of right: (str: 'ColumnOrName', len: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Right(str column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("right", str, len))
}

// Mask is the Golang equivalent of mask: (col: 'ColumnOrName', upperChar: Optional[ForwardRef('ColumnOrName')] = None, lowerChar: Optional[ForwardRef('ColumnOrName')] = None, digitChar: Optional[ForwardRef('ColumnOrName')] = None, otherChar: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Mask(col column.Column, upperChar column.Column, lowerChar column.Column, digitChar column.Column, otherChar column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mask", col, upperChar, lowerChar, digitChar, otherChar))
}

// Curdate is the Golang equivalent of curdate: () -> pyspark.sql.connect.column.Column
func Curdate() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("curdate"))
}

// CurrentDate is the Golang equivalent of current_date: () -> pyspark.sql.connect.column.Column
func CurrentDate() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_date"))
}

// CurrentTimestamp is the Golang equivalent of current_timestamp: () -> pyspark.sql.connect.column.Column
func CurrentTimestamp() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_timestamp"))
}

// Now is the Golang equivalent of now: () -> pyspark.sql.connect.column.Column
func Now() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("now"))
}

// CurrentTimezone is the Golang equivalent of current_timezone: () -> pyspark.sql.connect.column.Column
func CurrentTimezone() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_timezone"))
}

// Localtimestamp is the Golang equivalent of localtimestamp: () -> pyspark.sql.connect.column.Column
func Localtimestamp() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("localtimestamp"))
}

// DateFormat is the Golang equivalent of date_format: (date: 'ColumnOrName', format: str) -> pyspark.sql.connect.column.Column
func DateFormat(date column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_format", date, lit_format))
}

// Year is the Golang equivalent of year: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Year(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("year", col))
}

// Quarter is the Golang equivalent of quarter: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Quarter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("quarter", col))
}

// Month is the Golang equivalent of month: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Month(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("month", col))
}

// Dayofweek is the Golang equivalent of dayofweek: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofweek(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofweek", col))
}

// Dayofmonth is the Golang equivalent of dayofmonth: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofmonth(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofmonth", col))
}

// Day is the Golang equivalent of day: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Day(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("day", col))
}

// Dayofyear is the Golang equivalent of dayofyear: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofyear(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofyear", col))
}

// Hour is the Golang equivalent of hour: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hour(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hour", col))
}

// Minute is the Golang equivalent of minute: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Minute(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("minute", col))
}

// Second is the Golang equivalent of second: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Second(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("second", col))
}

// Weekofyear is the Golang equivalent of weekofyear: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Weekofyear(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("weekofyear", col))
}

// Weekday is the Golang equivalent of weekday: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Weekday(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("weekday", col))
}

// Extract is the Golang equivalent of extract: (field: 'ColumnOrName', source: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Extract(field column.Column, source column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("extract", field, source))
}

// DatePart is the Golang equivalent of date_part: (field: 'ColumnOrName', source: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DatePart(field column.Column, source column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_part", field, source))
}

// Datepart is the Golang equivalent of datepart: (field: 'ColumnOrName', source: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Datepart(field column.Column, source column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("datepart", field, source))
}

// MakeDate is the Golang equivalent of make_date: (year: 'ColumnOrName', month: 'ColumnOrName', day: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MakeDate(year column.Column, month column.Column, day column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_date", year, month, day))
}

// DateAdd is the Golang equivalent of date_add: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func DateAdd(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_add", start, days))
}

// Dateadd is the Golang equivalent of dateadd: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Dateadd(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dateadd", start, days))
}

// DateSub is the Golang equivalent of date_sub: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func DateSub(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_sub", start, days))
}

// Datediff is the Golang equivalent of datediff: (end: 'ColumnOrName', start: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Datediff(end column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("datediff", end, start))
}

// DateDiff is the Golang equivalent of date_diff: (end: 'ColumnOrName', start: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateDiff(end column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_diff", end, start))
}

// DateFromUnixDate is the Golang equivalent of date_from_unix_date: (days: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateFromUnixDate(days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_from_unix_date", days))
}

// AddMonths is the Golang equivalent of add_months: (start: 'ColumnOrName', months: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func AddMonths(start column.Column, months column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("add_months", start, months))
}

// TODO: months_between: (date1: 'ColumnOrName', date2: 'ColumnOrName', roundOff: bool = True) -> pyspark.sql.connect.column.Column

// ToDate is the Golang equivalent of to_date: (col: 'ColumnOrName', format: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ToDate(col column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_date", col, lit_format))
}

// UnixDate is the Golang equivalent of unix_date: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixDate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_date", col))
}

// UnixMicros is the Golang equivalent of unix_micros: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixMicros(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_micros", col))
}

// UnixMillis is the Golang equivalent of unix_millis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixMillis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_millis", col))
}

// UnixSeconds is the Golang equivalent of unix_seconds: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixSeconds(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_seconds", col))
}

// ToTimestamp is the Golang equivalent of to_timestamp: (col: 'ColumnOrName', format: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ToTimestamp(col column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp", col, lit_format))
}

// TryToTimestamp is the Golang equivalent of try_to_timestamp: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryToTimestamp(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_timestamp", col, format))
}

// Xpath is the Golang equivalent of xpath: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Xpath(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath", xml, path))
}

// XpathBoolean is the Golang equivalent of xpath_boolean: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathBoolean(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_boolean", xml, path))
}

// XpathDouble is the Golang equivalent of xpath_double: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathDouble(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_double", xml, path))
}

// XpathNumber is the Golang equivalent of xpath_number: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathNumber(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_number", xml, path))
}

// XpathFloat is the Golang equivalent of xpath_float: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathFloat(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_float", xml, path))
}

// XpathInt is the Golang equivalent of xpath_int: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathInt(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_int", xml, path))
}

// XpathLong is the Golang equivalent of xpath_long: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathLong(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_long", xml, path))
}

// XpathShort is the Golang equivalent of xpath_short: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathShort(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_short", xml, path))
}

// XpathString is the Golang equivalent of xpath_string: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathString(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_string", xml, path))
}

// Trunc is the Golang equivalent of trunc: (date: 'ColumnOrName', format: str) -> pyspark.sql.connect.column.Column
func Trunc(date column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("trunc", date, lit_format))
}

// DateTrunc is the Golang equivalent of date_trunc: (format: str, timestamp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateTrunc(format string, timestamp column.Column) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_trunc", lit_format, timestamp))
}

// NextDay is the Golang equivalent of next_day: (date: 'ColumnOrName', dayOfWeek: str) -> pyspark.sql.connect.column.Column
func NextDay(date column.Column, dayOfWeek string) column.Column {
	lit_dayOfWeek := Lit(dayOfWeek)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("next_day", date, lit_dayOfWeek))
}

// LastDay is the Golang equivalent of last_day: (date: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func LastDay(date column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("last_day", date))
}

// FromUnixtime is the Golang equivalent of from_unixtime: (timestamp: 'ColumnOrName', format: str = 'yyyy-MM-dd HH:mm:ss') -> pyspark.sql.connect.column.Column
func FromUnixtime(timestamp column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("from_unixtime", timestamp, lit_format))
}

// UnixTimestamp is the Golang equivalent of unix_timestamp: (timestamp: Optional[ForwardRef('ColumnOrName')] = None, format: str = 'yyyy-MM-dd HH:mm:ss') -> pyspark.sql.connect.column.Column
func UnixTimestamp(timestamp column.Column, format string) column.Column {
	lit_format := Lit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_timestamp", timestamp, lit_format))
}

// FromUtcTimestamp is the Golang equivalent of from_utc_timestamp: (timestamp: 'ColumnOrName', tz: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FromUtcTimestamp(timestamp column.Column, tz column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("from_utc_timestamp", timestamp, tz))
}

// ToUtcTimestamp is the Golang equivalent of to_utc_timestamp: (timestamp: 'ColumnOrName', tz: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToUtcTimestamp(timestamp column.Column, tz column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_utc_timestamp", timestamp, tz))
}

// TimestampSeconds is the Golang equivalent of timestamp_seconds: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampSeconds(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_seconds", col))
}

// TimestampMillis is the Golang equivalent of timestamp_millis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampMillis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_millis", col))
}

// TimestampMicros is the Golang equivalent of timestamp_micros: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampMicros(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_micros", col))
}

// Window is the Golang equivalent of window: (timeColumn: 'ColumnOrName', windowDuration: str, slideDuration: Optional[str] = None, startTime: Optional[str] = None) -> pyspark.sql.connect.column.Column
func Window(timeColumn column.Column, windowDuration string, slideDuration string, startTime string) column.Column {
	lit_windowDuration := Lit(windowDuration)
	lit_slideDuration := Lit(slideDuration)
	lit_startTime := Lit(startTime)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("window", timeColumn, lit_windowDuration, lit_slideDuration, lit_startTime))
}

// WindowTime is the Golang equivalent of window_time: (windowColumn: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func WindowTime(windowColumn column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("window_time", windowColumn))
}

// TODO: session_window: (timeColumn: 'ColumnOrName', gapDuration: Union[pyspark.sql.connect.column.Column, str]) -> pyspark.sql.connect.column.Column

// ToUnixTimestamp is the Golang equivalent of to_unix_timestamp: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToUnixTimestamp(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_unix_timestamp", timestamp, format))
}

// ToTimestampLtz is the Golang equivalent of to_timestamp_ltz: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToTimestampLtz(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp_ltz", timestamp, format))
}

// ToTimestampNtz is the Golang equivalent of to_timestamp_ntz: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToTimestampNtz(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp_ntz", timestamp, format))
}

// TODO: bucket: (numBuckets: Union[pyspark.sql.connect.column.Column, int], col: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// Years is the Golang equivalent of years: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Years(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("years", col))
}

// Months is the Golang equivalent of months: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Months(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("months", col))
}

// Days is the Golang equivalent of days: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Days(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("days", col))
}

// Hours is the Golang equivalent of hours: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hours(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hours", col))
}

// TODO: convert_timezone: (sourceTz: Optional[pyspark.sql.connect.column.Column], targetTz: pyspark.sql.connect.column.Column, sourceTs: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// MakeDtInterval is the Golang equivalent of make_dt_interval: (days: Optional[ForwardRef('ColumnOrName')] = None, hours: Optional[ForwardRef('ColumnOrName')] = None, mins: Optional[ForwardRef('ColumnOrName')] = None, secs: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeDtInterval(days column.Column, hours column.Column, mins column.Column, secs column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_dt_interval", days, hours, mins, secs))
}

// MakeInterval is the Golang equivalent of make_interval: (years: Optional[ForwardRef('ColumnOrName')] = None, months: Optional[ForwardRef('ColumnOrName')] = None, weeks: Optional[ForwardRef('ColumnOrName')] = None, days: Optional[ForwardRef('ColumnOrName')] = None, hours: Optional[ForwardRef('ColumnOrName')] = None, mins: Optional[ForwardRef('ColumnOrName')] = None, secs: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeInterval(years column.Column, months column.Column, weeks column.Column, days column.Column, hours column.Column, mins column.Column, secs column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_interval", years, months, weeks, days, hours, mins, secs))
}

// MakeTimestamp is the Golang equivalent of make_timestamp: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName', timezone: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeTimestamp(years column.Column, months column.Column, days column.Column, hours column.Column, mins column.Column, secs column.Column, timezone column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp", years, months, days, hours, mins, secs, timezone))
}

// MakeTimestampLtz is the Golang equivalent of make_timestamp_ltz: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName', timezone: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeTimestampLtz(years column.Column, months column.Column, days column.Column, hours column.Column, mins column.Column, secs column.Column, timezone column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp_ltz", years, months, days, hours, mins, secs, timezone))
}

// MakeTimestampNtz is the Golang equivalent of make_timestamp_ntz: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MakeTimestampNtz(years column.Column, months column.Column, days column.Column, hours column.Column, mins column.Column, secs column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp_ntz", years, months, days, hours, mins, secs))
}

// MakeYmInterval is the Golang equivalent of make_ym_interval: (years: Optional[ForwardRef('ColumnOrName')] = None, months: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeYmInterval(years column.Column, months column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_ym_interval", years, months))
}

// CurrentCatalog is the Golang equivalent of current_catalog: () -> pyspark.sql.connect.column.Column
func CurrentCatalog() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_catalog"))
}

// CurrentDatabase is the Golang equivalent of current_database: () -> pyspark.sql.connect.column.Column
func CurrentDatabase() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_database"))
}

// CurrentSchema is the Golang equivalent of current_schema: () -> pyspark.sql.connect.column.Column
func CurrentSchema() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_schema"))
}

// CurrentUser is the Golang equivalent of current_user: () -> pyspark.sql.connect.column.Column
func CurrentUser() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_user"))
}

// User is the Golang equivalent of user: () -> pyspark.sql.connect.column.Column
func User() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("user"))
}

// TODO: assert_true: (col: 'ColumnOrName', errMsg: Union[pyspark.sql.connect.column.Column, str, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: raise_error: (errMsg: Union[pyspark.sql.connect.column.Column, str]) -> pyspark.sql.connect.column.Column

// Crc32 is the Golang equivalent of crc32: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Crc32(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("crc32", col))
}

// Hash is the Golang equivalent of hash: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hash(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hash", vals...))
}

// Xxhash64 is the Golang equivalent of xxhash64: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Xxhash64(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xxhash64", vals...))
}

// Md5 is the Golang equivalent of md5: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Md5(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("md5", col))
}

// Sha1 is the Golang equivalent of sha1: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sha1(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha1", col))
}

// Sha2 is the Golang equivalent of sha2: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Sha2(col column.Column, numBits int64) column.Column {
	lit_numBits := Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha2", col, lit_numBits))
}

// TODO: hll_sketch_agg: (col: 'ColumnOrName', lgConfigK: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: hll_union_agg: (col: 'ColumnOrName', allowDifferentLgConfigK: Optional[bool] = None) -> pyspark.sql.connect.column.Column

// HllSketchEstimate is the Golang equivalent of hll_sketch_estimate: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func HllSketchEstimate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hll_sketch_estimate", col))
}

// TODO: hll_union: (col1: 'ColumnOrName', col2: 'ColumnOrName', allowDifferentLgConfigK: Optional[bool] = None) -> pyspark.sql.connect.column.Column

// Ifnull is the Golang equivalent of ifnull: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ifnull(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ifnull", col1, col2))
}

// Isnotnull is the Golang equivalent of isnotnull: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnotnull(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnotnull", col))
}

// EqualNull is the Golang equivalent of equal_null: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func EqualNull(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("equal_null", col1, col2))
}

// Nullif is the Golang equivalent of nullif: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nullif(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nullif", col1, col2))
}

// Nvl is the Golang equivalent of nvl: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nvl(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nvl", col1, col2))
}

// Nvl2 is the Golang equivalent of nvl2: (col1: 'ColumnOrName', col2: 'ColumnOrName', col3: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nvl2(col1 column.Column, col2 column.Column, col3 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nvl2", col1, col2, col3))
}

// AesEncrypt is the Golang equivalent of aes_encrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, iv: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func AesEncrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, iv column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("aes_encrypt", input, key, mode, padding, iv, aad))
}

// AesDecrypt is the Golang equivalent of aes_decrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func AesDecrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("aes_decrypt", input, key, mode, padding, aad))
}

// TryAesDecrypt is the Golang equivalent of try_aes_decrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryAesDecrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_aes_decrypt", input, key, mode, padding, aad))
}

// Sha is the Golang equivalent of sha: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sha(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha", col))
}

// InputFileBlockLength is the Golang equivalent of input_file_block_length: () -> pyspark.sql.connect.column.Column
func InputFileBlockLength() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_block_length"))
}

// InputFileBlockStart is the Golang equivalent of input_file_block_start: () -> pyspark.sql.connect.column.Column
func InputFileBlockStart() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_block_start"))
}

// Reflect is the Golang equivalent of reflect: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Reflect(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("reflect", vals...))
}

// JavaMethod is the Golang equivalent of java_method: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JavaMethod(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("java_method", vals...))
}

// Version is the Golang equivalent of version: () -> pyspark.sql.connect.column.Column
func Version() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("version"))
}

// Typeof is the Golang equivalent of typeof: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Typeof(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("typeof", col))
}

// Stack is the Golang equivalent of stack: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Stack(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stack", vals...))
}

// BitmapBitPosition is the Golang equivalent of bitmap_bit_position: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapBitPosition(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_bit_position", col))
}

// BitmapBucketNumber is the Golang equivalent of bitmap_bucket_number: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapBucketNumber(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_bucket_number", col))
}

// BitmapConstructAgg is the Golang equivalent of bitmap_construct_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapConstructAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_construct_agg", col))
}

// BitmapCount is the Golang equivalent of bitmap_count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapCount(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_count", col))
}

// BitmapOrAgg is the Golang equivalent of bitmap_or_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapOrAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_or_agg", col))
}

// Ignore UDF: call_udf: (udfName: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// Ignore UDT: unwrap_udt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// TODO: udf: (f: Union[Callable[..., Any], ForwardRef('DataTypeOrString'), NoneType] = None, returnType: 'DataTypeOrString' = StringType(), useArrow: Optional[bool] = None) -> Union[ForwardRef('UserDefinedFunctionLike'), Callable[[Callable[..., Any]], ForwardRef('UserDefinedFunctionLike')]]

// Ignore UDT: udtf: (cls: Optional[Type] = None, *, returnType: Union[pyspark.sql.types.StructType, str], useArrow: Optional[bool] = None) -> Union[ForwardRef('UserDefinedTableFunction'), Callable[[Type], ForwardRef('UserDefinedTableFunction')]]

// CallFunction is the Golang equivalent of call_function: (funcName: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CallFunction(funcName string, cols ...column.Column) column.Column {
	lit_funcName := Lit(funcName)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_funcName)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("call_function", vals...))
}
