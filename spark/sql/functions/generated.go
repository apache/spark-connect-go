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

import "github.com/apache/spark-connect-go/v40/spark/sql/column"

// BitwiseNOT - Computes bitwise not.
//
// BitwiseNOT is the Golang equivalent of bitwiseNOT: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitwiseNOT(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitwiseNOT", col))
}

// BitwiseNot - Computes bitwise not.
//
// BitwiseNot is the Golang equivalent of bitwise_not: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitwiseNot(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitwise_not", col))
}

// BitCount - Returns the number of bits that are set in the argument expr as an unsigned 64-bit integer,
// or NULL if the argument is NULL.
//
// BitCount is the Golang equivalent of bit_count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitCount(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_count", col))
}

// BitGet - Returns the value of the bit (0 or 1) at the specified position.
// The positions are numbered from right to left, starting at zero.
// The position argument cannot be negative.
//
// BitGet is the Golang equivalent of bit_get: (col: 'ColumnOrName', pos: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitGet(col column.Column, pos column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_get", col, pos))
}

// Getbit - Returns the value of the bit (0 or 1) at the specified position.
// The positions are numbered from right to left, starting at zero.
// The position argument cannot be negative.
//
// Getbit is the Golang equivalent of getbit: (col: 'ColumnOrName', pos: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Getbit(col column.Column, pos column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("getbit", col, pos))
}

// TODO: broadcast: (df: 'DataFrame') -> 'DataFrame'

// Coalesce - Returns the first column that is not null.
//
// Coalesce is the Golang equivalent of coalesce: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Coalesce(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("coalesce", vals...))
}

// Greatest - Returns the greatest value of the list of column names, skipping null values.
// This function takes at least 2 parameters. It will return null if all parameters are null.
//
// Greatest is the Golang equivalent of greatest: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Greatest(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("greatest", vals...))
}

// InputFileName - Creates a string column for the file name of the current Spark task.
//
// InputFileName is the Golang equivalent of input_file_name: () -> pyspark.sql.connect.column.Column
func InputFileName() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_name"))
}

// Least - Returns the least value of the list of column names, skipping null values.
// This function takes at least 2 parameters. It will return null if all parameters are null.
//
// Least is the Golang equivalent of least: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Least(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("least", vals...))
}

// Isnan - An expression that returns true if the column is NaN.
//
// Isnan is the Golang equivalent of isnan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnan", col))
}

// Isnull - An expression that returns true if the column is null.
//
// Isnull is the Golang equivalent of isnull: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnull(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnull", col))
}

// MonotonicallyIncreasingId - A column that generates monotonically increasing 64-bit integers.
//
// The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
// The current implementation puts the partition ID in the upper 31 bits, and the record number
// within each partition in the lower 33 bits. The assumption is that the data frame has
// less than 1 billion partitions, and each partition has less than 8 billion records.
//
// MonotonicallyIncreasingId is the Golang equivalent of monotonically_increasing_id: () -> pyspark.sql.connect.column.Column
func MonotonicallyIncreasingId() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("monotonically_increasing_id"))
}

// Nanvl - Returns col1 if it is not NaN, or col2 if col1 is NaN.
//
// Both inputs should be floating point columns (:class:`DoubleType` or :class:`FloatType`).
//
// Nanvl is the Golang equivalent of nanvl: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nanvl(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nanvl", col1, col2))
}

// Rand - Generates a random column with independent and identically distributed (i.i.d.) samples
// uniformly distributed in [0.0, 1.0).
//
// Rand is the Golang equivalent of rand: (seed: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Rand(seed int64) column.Column {
	lit_seed := Int64Lit(seed)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rand", lit_seed))
}

// Randn - Generates a column with independent and identically distributed (i.i.d.) samples from
// the standard normal distribution.
//
// Randn is the Golang equivalent of randn: (seed: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Randn(seed int64) column.Column {
	lit_seed := Int64Lit(seed)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("randn", lit_seed))
}

// SparkPartitionId - A column for partition ID.
//
// SparkPartitionId is the Golang equivalent of spark_partition_id: () -> pyspark.sql.connect.column.Column
func SparkPartitionId() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("spark_partition_id"))
}

// TODO: when: (condition: pyspark.sql.connect.column.Column, value: Any) -> pyspark.sql.connect.column.Column

// Asc - Returns a sort expression based on the ascending order of the given column name.
//
// Asc is the Golang equivalent of asc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc", col))
}

// AscNullsFirst - Returns a sort expression based on the ascending order of the given
// column name, and null values return before non-null values.
//
// AscNullsFirst is the Golang equivalent of asc_nulls_first: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func AscNullsFirst(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc_nulls_first", col))
}

// AscNullsLast - Returns a sort expression based on the ascending order of the given
// column name, and null values appear after non-null values.
//
// AscNullsLast is the Golang equivalent of asc_nulls_last: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func AscNullsLast(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asc_nulls_last", col))
}

// Desc - Returns a sort expression based on the descending order of the given column name.
//
// Desc is the Golang equivalent of desc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Desc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc", col))
}

// DescNullsFirst - Returns a sort expression based on the descending order of the given
// column name, and null values appear before non-null values.
//
// DescNullsFirst is the Golang equivalent of desc_nulls_first: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DescNullsFirst(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc_nulls_first", col))
}

// DescNullsLast - Returns a sort expression based on the descending order of the given
// column name, and null values appear after non-null values.
//
// DescNullsLast is the Golang equivalent of desc_nulls_last: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DescNullsLast(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("desc_nulls_last", col))
}

// Abs - Computes the absolute value.
//
// Abs is the Golang equivalent of abs: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Abs(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("abs", col))
}

// Acos - Computes inverse cosine of the input column.
//
// Acos is the Golang equivalent of acos: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Acos(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("acos", col))
}

// Acosh - Computes inverse hyperbolic cosine of the input column.
//
// Acosh is the Golang equivalent of acosh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Acosh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("acosh", col))
}

// Asin - Computes inverse sine of the input column.
//
// Asin is the Golang equivalent of asin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asin", col))
}

// Asinh - Computes inverse hyperbolic sine of the input column.
//
// Asinh is the Golang equivalent of asinh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Asinh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("asinh", col))
}

// Atan - Compute inverse tangent of the input column.
//
// Atan is the Golang equivalent of atan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Atan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atan", col))
}

// Atan2 -
//
// Atan2 is the Golang equivalent of atan2: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Atan2(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atan2", col1, col2))
}

// Atanh - Computes inverse hyperbolic tangent of the input column.
//
// Atanh is the Golang equivalent of atanh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Atanh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("atanh", col))
}

// Bin - Returns the string representation of the binary value of the given column.
//
// Bin is the Golang equivalent of bin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Bin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bin", col))
}

// Bround - Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0
// or at integral part when `scale` < 0.
//
// Bround is the Golang equivalent of bround: (col: 'ColumnOrName', scale: int = 0) -> pyspark.sql.connect.column.Column
func Bround(col column.Column, scale int64) column.Column {
	lit_scale := Int64Lit(scale)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bround", col, lit_scale))
}

// Cbrt - Computes the cube-root of the given value.
//
// Cbrt is the Golang equivalent of cbrt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cbrt(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cbrt", col))
}

// Ceil - Computes the ceiling of the given value.
//
// Ceil is the Golang equivalent of ceil: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ceil(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ceil", col))
}

// Ceiling - Computes the ceiling of the given value.
//
// Ceiling is the Golang equivalent of ceiling: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ceiling(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ceiling", col))
}

// Conv - Convert a number in a string column from one base to another.
//
// Conv is the Golang equivalent of conv: (col: 'ColumnOrName', fromBase: int, toBase: int) -> pyspark.sql.connect.column.Column
func Conv(col column.Column, fromBase int64, toBase int64) column.Column {
	lit_fromBase := Int64Lit(fromBase)
	lit_toBase := Int64Lit(toBase)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("conv", col, lit_fromBase, lit_toBase))
}

// Cos - Computes cosine of the input column.
//
// Cos is the Golang equivalent of cos: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cos(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cos", col))
}

// Cosh - Computes hyperbolic cosine of the input column.
//
// Cosh is the Golang equivalent of cosh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cosh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cosh", col))
}

// Cot - Computes cotangent of the input column.
//
// Cot is the Golang equivalent of cot: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cot(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cot", col))
}

// Csc - Computes cosecant of the input column.
//
// Csc is the Golang equivalent of csc: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Csc(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("csc", col))
}

// Degrees - Converts an angle measured in radians to an approximately equivalent angle
// measured in degrees.
//
// Degrees is the Golang equivalent of degrees: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Degrees(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("degrees", col))
}

// E - Returns Euler's number.
//
// E is the Golang equivalent of e: () -> pyspark.sql.connect.column.Column
func E() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("e"))
}

// Exp - Computes the exponential of the given value.
//
// Exp is the Golang equivalent of exp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Exp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("exp", col))
}

// Expm1 - Computes the exponential of the given value minus one.
//
// Expm1 is the Golang equivalent of expm1: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Expm1(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("expm1", col))
}

// Factorial - Computes the factorial of the given value.
//
// Factorial is the Golang equivalent of factorial: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Factorial(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("factorial", col))
}

// Floor - Computes the floor of the given value.
//
// Floor is the Golang equivalent of floor: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Floor(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("floor", col))
}

// Hex - Computes hex value of the given column, which could be :class:`pyspark.sql.types.StringType`,
// :class:`pyspark.sql.types.BinaryType`, :class:`pyspark.sql.types.IntegerType` or
// :class:`pyspark.sql.types.LongType`.
//
// Hex is the Golang equivalent of hex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hex", col))
}

// Hypot - Computes “sqrt(a^2 + b^2)“ without intermediate overflow or underflow.
//
// Hypot is the Golang equivalent of hypot: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Hypot(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hypot", col1, col2))
}

// Log - Returns the first argument-based logarithm of the second argument.
//
// If there is only one argument, then this takes the natural logarithm of the argument.
//
// Log is the Golang equivalent of log: (arg1: Union[ForwardRef('ColumnOrName'), float], arg2: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Log(arg1 column.Column, arg2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log", arg1, arg2))
}

// Log10 - Computes the logarithm of the given value in Base 10.
//
// Log10 is the Golang equivalent of log10: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log10(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log10", col))
}

// Log1p - Computes the natural logarithm of the "given value plus one".
//
// Log1p is the Golang equivalent of log1p: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log1p(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log1p", col))
}

// Ln - Returns the natural logarithm of the argument.
//
// Ln is the Golang equivalent of ln: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ln(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ln", col))
}

// Log2 - Returns the base-2 logarithm of the argument.
//
// Log2 is the Golang equivalent of log2: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Log2(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("log2", col))
}

// Negative - Returns the negative value.
//
// Negative is the Golang equivalent of negative: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Negative(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("negative", col))
}

// Negate - Returns the negative value.
//
// Negate is the Golang equivalent of negate: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Negate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("negate", col))
}

// Pi - Returns Pi.
//
// Pi is the Golang equivalent of pi: () -> pyspark.sql.connect.column.Column
func Pi() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pi"))
}

// Positive - Returns the value.
//
// Positive is the Golang equivalent of positive: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Positive(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("positive", col))
}

// Pmod - Returns the positive value of dividend mod divisor.
//
// Pmod is the Golang equivalent of pmod: (dividend: Union[ForwardRef('ColumnOrName'), float], divisor: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Pmod(dividend column.Column, divisor column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pmod", dividend, divisor))
}

// WidthBucket - Returns the bucket number into which the value of this expression would fall
// after being evaluated. Note that input arguments must follow conditions listed below;
// otherwise, the method will return null.
//
// WidthBucket is the Golang equivalent of width_bucket: (v: 'ColumnOrName', min: 'ColumnOrName', max: 'ColumnOrName', numBucket: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func WidthBucket(v column.Column, min column.Column, max column.Column, numBucket column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("width_bucket", v, min, max, numBucket))
}

// Pow - Returns the value of the first argument raised to the power of the second argument.
//
// Pow is the Golang equivalent of pow: (col1: Union[ForwardRef('ColumnOrName'), float], col2: Union[ForwardRef('ColumnOrName'), float]) -> pyspark.sql.connect.column.Column
func Pow(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("pow", col1, col2))
}

// Radians - Converts an angle measured in degrees to an approximately equivalent angle
// measured in radians.
//
// Radians is the Golang equivalent of radians: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Radians(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("radians", col))
}

// Rint - Returns the double value that is closest in value to the argument and
// is equal to a mathematical integer.
//
// Rint is the Golang equivalent of rint: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rint(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rint", col))
}

// Round - Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0
// or at integral part when `scale` < 0.
//
// Round is the Golang equivalent of round: (col: 'ColumnOrName', scale: int = 0) -> pyspark.sql.connect.column.Column
func Round(col column.Column, scale int64) column.Column {
	lit_scale := Int64Lit(scale)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("round", col, lit_scale))
}

// Sec - Computes secant of the input column.
//
// Sec is the Golang equivalent of sec: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sec(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sec", col))
}

// ShiftLeft - Shift the given value numBits left.
//
// ShiftLeft is the Golang equivalent of shiftLeft: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftLeft(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftLeft", col, lit_numBits))
}

// Shiftleft - Shift the given value numBits left.
//
// Shiftleft is the Golang equivalent of shiftleft: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftleft(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftleft", col, lit_numBits))
}

// ShiftRight - (Signed) shift the given value numBits right.
//
// ShiftRight is the Golang equivalent of shiftRight: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftRight(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftRight", col, lit_numBits))
}

// Shiftright - (Signed) shift the given value numBits right.
//
// Shiftright is the Golang equivalent of shiftright: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftright(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftright", col, lit_numBits))
}

// ShiftRightUnsigned - Unsigned shift the given value numBits right.
//
// ShiftRightUnsigned is the Golang equivalent of shiftRightUnsigned: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func ShiftRightUnsigned(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftRightUnsigned", col, lit_numBits))
}

// Shiftrightunsigned - Unsigned shift the given value numBits right.
//
// Shiftrightunsigned is the Golang equivalent of shiftrightunsigned: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Shiftrightunsigned(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shiftrightunsigned", col, lit_numBits))
}

// Signum - Computes the signum of the given value.
//
// Signum is the Golang equivalent of signum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Signum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("signum", col))
}

// Sign - Computes the signum of the given value.
//
// Sign is the Golang equivalent of sign: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sign(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sign", col))
}

// Sin - Computes sine of the input column.
//
// Sin is the Golang equivalent of sin: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sin", col))
}

// Sinh - Computes hyperbolic sine of the input column.
//
// Sinh is the Golang equivalent of sinh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sinh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sinh", col))
}

// Sqrt - Computes the square root of the specified float value.
//
// Sqrt is the Golang equivalent of sqrt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sqrt(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sqrt", col))
}

// TryAdd - Returns the sum of `left`and `right` and the result is null on overflow.
// The acceptable input types are the same with the `+` operator.
//
// TryAdd is the Golang equivalent of try_add: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryAdd(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_add", left, right))
}

// TryAvg - Returns the mean calculated from values of a group and the result is null on overflow.
//
// TryAvg is the Golang equivalent of try_avg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryAvg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_avg", col))
}

// TryDivide - Returns `dividend`/`divisor`. It always performs floating point division. Its result is
// always null if `divisor` is 0.
//
// TryDivide is the Golang equivalent of try_divide: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryDivide(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_divide", left, right))
}

// TryMultiply - Returns `left`*`right` and the result is null on overflow. The acceptable input types are the
// same with the `*` operator.
//
// TryMultiply is the Golang equivalent of try_multiply: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryMultiply(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_multiply", left, right))
}

// TrySubtract - Returns `left`-`right` and the result is null on overflow. The acceptable input types are the
// same with the `-` operator.
//
// TrySubtract is the Golang equivalent of try_subtract: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TrySubtract(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_subtract", left, right))
}

// TrySum - Returns the sum calculated from values of a group and the result is null on overflow.
//
// TrySum is the Golang equivalent of try_sum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TrySum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_sum", col))
}

// Tan - Computes tangent of the input column.
//
// Tan is the Golang equivalent of tan: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Tan(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("tan", col))
}

// Tanh - Computes hyperbolic tangent of the input column.
//
// Tanh is the Golang equivalent of tanh: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Tanh(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("tanh", col))
}

// ToDegrees -
//
// ToDegrees is the Golang equivalent of toDegrees: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToDegrees(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("toDegrees", col))
}

// ToRadians -
//
// ToRadians is the Golang equivalent of toRadians: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToRadians(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("toRadians", col))
}

// Unhex - Inverse of hex. Interprets each pair of characters as a hexadecimal number
// and converts to the byte representation of number.
//
// Unhex is the Golang equivalent of unhex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Unhex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unhex", col))
}

// ApproxCountDistinct - Aggregate function: returns a new :class:`~pyspark.sql.Column` for approximate distinct count
// of column `col`.
//
// ApproxCountDistinct is the Golang equivalent of approx_count_distinct: (col: 'ColumnOrName', rsd: Optional[float] = None) -> pyspark.sql.connect.column.Column
func ApproxCountDistinct(col column.Column, rsd float64) column.Column {
	lit_rsd := Float64Lit(rsd)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("approx_count_distinct", col, lit_rsd))
}

// Avg - Aggregate function: returns the average of the values in a group.
//
// Avg is the Golang equivalent of avg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Avg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("avg", col))
}

// CollectList - Aggregate function: returns a list of objects with duplicates.
//
// CollectList is the Golang equivalent of collect_list: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CollectList(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("collect_list", col))
}

// ArrayAgg - Aggregate function: returns a list of objects with duplicates.
//
// ArrayAgg is the Golang equivalent of array_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_agg", col))
}

// CollectSet - Aggregate function: returns a set of objects with duplicate elements eliminated.
//
// CollectSet is the Golang equivalent of collect_set: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CollectSet(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("collect_set", col))
}

// Corr - Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for
// “col1“ and “col2“.
//
// Corr is the Golang equivalent of corr: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Corr(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("corr", col1, col2))
}

// Count - Aggregate function: returns the number of items in a group.
//
// Count is the Golang equivalent of count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Count(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count", col))
}

// CountDistinct - Returns a new :class:`Column` for distinct count of “col“ or “cols“.
//
// CountDistinct is the Golang equivalent of count_distinct: (col: 'ColumnOrName', *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountDistinct(col column.Column, cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, col)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_distinct", vals...))
}

// CovarPop - Returns a new :class:`~pyspark.sql.Column` for the population covariance of “col1“ and
// “col2“.
//
// CovarPop is the Golang equivalent of covar_pop: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CovarPop(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("covar_pop", col1, col2))
}

// CovarSamp - Returns a new :class:`~pyspark.sql.Column` for the sample covariance of “col1“ and
// “col2“.
//
// CovarSamp is the Golang equivalent of covar_samp: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CovarSamp(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("covar_samp", col1, col2))
}

// TODO: first: (col: 'ColumnOrName', ignorenulls: bool = False) -> pyspark.sql.connect.column.Column

// Grouping - Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated
// or not, returns 1 for aggregated or 0 for not aggregated in the result set.
//
// Grouping is the Golang equivalent of grouping: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Grouping(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("grouping", col))
}

// GroupingId - Aggregate function: returns the level of grouping, equals to
//
// (grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)
//
// GroupingId is the Golang equivalent of grouping_id: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func GroupingId(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("grouping_id", vals...))
}

// CountMinSketch - Returns a count-min sketch of a column with the given esp, confidence and seed.
// The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage.
// Count-min sketch is a probabilistic data structure used for cardinality estimation
// using sub-linear space.
//
// CountMinSketch is the Golang equivalent of count_min_sketch: (col: 'ColumnOrName', eps: 'ColumnOrName', confidence: 'ColumnOrName', seed: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountMinSketch(col column.Column, eps column.Column, confidence column.Column, seed column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_min_sketch", col, eps, confidence, seed))
}

// Kurtosis - Aggregate function: returns the kurtosis of the values in a group.
//
// Kurtosis is the Golang equivalent of kurtosis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Kurtosis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("kurtosis", col))
}

// TODO: last: (col: 'ColumnOrName', ignorenulls: bool = False) -> pyspark.sql.connect.column.Column

// Max - Aggregate function: returns the maximum value of the expression in a group.
//
// Max is the Golang equivalent of max: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Max(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("max", col))
}

// MaxBy - Returns the value associated with the maximum value of ord.
//
// MaxBy is the Golang equivalent of max_by: (col: 'ColumnOrName', ord: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MaxBy(col column.Column, ord column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("max_by", col, ord))
}

// Mean - Aggregate function: returns the average of the values in a group.
// An alias of :func:`avg`.
//
// Mean is the Golang equivalent of mean: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Mean(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mean", col))
}

// Median - Returns the median of the values in a group.
//
// Median is the Golang equivalent of median: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Median(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("median", col))
}

// Min - Aggregate function: returns the minimum value of the expression in a group.
//
// Min is the Golang equivalent of min: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Min(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("min", col))
}

// MinBy - Returns the value associated with the minimum value of ord.
//
// MinBy is the Golang equivalent of min_by: (col: 'ColumnOrName', ord: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MinBy(col column.Column, ord column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("min_by", col, ord))
}

// Mode - Returns the most frequent value in a group.
//
// Mode is the Golang equivalent of mode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Mode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mode", col))
}

// TODO: percentile: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], frequency: Union[pyspark.sql.connect.column.Column, int] = 1) -> pyspark.sql.connect.column.Column

// TODO: percentile_approx: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], accuracy: Union[pyspark.sql.connect.column.Column, float] = 10000) -> pyspark.sql.connect.column.Column

// TODO: approx_percentile: (col: 'ColumnOrName', percentage: Union[pyspark.sql.connect.column.Column, float, List[float], Tuple[float]], accuracy: Union[pyspark.sql.connect.column.Column, float] = 10000) -> pyspark.sql.connect.column.Column

// Product - Aggregate function: returns the product of the values in a group.
//
// Product is the Golang equivalent of product: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Product(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("product", col))
}

// Skewness - Aggregate function: returns the skewness of the values in a group.
//
// Skewness is the Golang equivalent of skewness: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Skewness(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("skewness", col))
}

// Stddev - Aggregate function: alias for stddev_samp.
//
// Stddev is the Golang equivalent of stddev: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Stddev(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev", col))
}

// Std - Aggregate function: alias for stddev_samp.
//
// Std is the Golang equivalent of std: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Std(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("std", col))
}

// StddevSamp - Aggregate function: returns the unbiased sample standard deviation of
// the expression in a group.
//
// StddevSamp is the Golang equivalent of stddev_samp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func StddevSamp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev_samp", col))
}

// StddevPop - Aggregate function: returns population standard deviation of
// the expression in a group.
//
// StddevPop is the Golang equivalent of stddev_pop: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func StddevPop(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stddev_pop", col))
}

// Sum - Aggregate function: returns the sum of all values in the expression.
//
// Sum is the Golang equivalent of sum: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sum(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sum", col))
}

// SumDistinct - Aggregate function: returns the sum of distinct values in the expression.
//
// SumDistinct is the Golang equivalent of sum_distinct: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func SumDistinct(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sum_distinct", col))
}

// VarPop - Aggregate function: returns the population variance of the values in a group.
//
// VarPop is the Golang equivalent of var_pop: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func VarPop(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("var_pop", col))
}

// RegrAvgx - Aggregate function: returns the average of the independent variable for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrAvgx is the Golang equivalent of regr_avgx: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrAvgx(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_avgx", y, x))
}

// RegrAvgy - Aggregate function: returns the average of the dependent variable for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrAvgy is the Golang equivalent of regr_avgy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrAvgy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_avgy", y, x))
}

// RegrCount - Aggregate function: returns the number of non-null number pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrCount is the Golang equivalent of regr_count: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrCount(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_count", y, x))
}

// RegrIntercept - Aggregate function: returns the intercept of the univariate linear regression line
// for non-null pairs in a group, where `y` is the dependent variable and
// `x` is the independent variable.
//
// RegrIntercept is the Golang equivalent of regr_intercept: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrIntercept(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_intercept", y, x))
}

// RegrR2 - Aggregate function: returns the coefficient of determination for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrR2 is the Golang equivalent of regr_r2: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrR2(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_r2", y, x))
}

// RegrSlope - Aggregate function: returns the slope of the linear regression line for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrSlope is the Golang equivalent of regr_slope: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSlope(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_slope", y, x))
}

// RegrSxx - Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrSxx is the Golang equivalent of regr_sxx: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSxx(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_sxx", y, x))
}

// RegrSxy - Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrSxy is the Golang equivalent of regr_sxy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSxy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_sxy", y, x))
}

// RegrSyy - Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs
// in a group, where `y` is the dependent variable and `x` is the independent variable.
//
// RegrSyy is the Golang equivalent of regr_syy: (y: 'ColumnOrName', x: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegrSyy(y column.Column, x column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regr_syy", y, x))
}

// VarSamp - Aggregate function: returns the unbiased sample variance of
// the values in a group.
//
// VarSamp is the Golang equivalent of var_samp: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func VarSamp(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("var_samp", col))
}

// Variance - Aggregate function: alias for var_samp
//
// Variance is the Golang equivalent of variance: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Variance(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("variance", col))
}

// Every - Aggregate function: returns true if all values of `col` are true.
//
// Every is the Golang equivalent of every: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Every(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("every", col))
}

// BoolAnd - Aggregate function: returns true if all values of `col` are true.
//
// BoolAnd is the Golang equivalent of bool_and: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BoolAnd(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bool_and", col))
}

// Some - Aggregate function: returns true if at least one value of `col` is true.
//
// Some is the Golang equivalent of some: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Some(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("some", col))
}

// BoolOr - Aggregate function: returns true if at least one value of `col` is true.
//
// BoolOr is the Golang equivalent of bool_or: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BoolOr(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bool_or", col))
}

// BitAnd - Aggregate function: returns the bitwise AND of all non-null input values, or null if none.
//
// BitAnd is the Golang equivalent of bit_and: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitAnd(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_and", col))
}

// BitOr - Aggregate function: returns the bitwise OR of all non-null input values, or null if none.
//
// BitOr is the Golang equivalent of bit_or: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitOr(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_or", col))
}

// BitXor - Aggregate function: returns the bitwise XOR of all non-null input values, or null if none.
//
// BitXor is the Golang equivalent of bit_xor: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitXor(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_xor", col))
}

// CumeDist - Window function: returns the cumulative distribution of values within a window partition,
// i.e. the fraction of rows that are below the current row.
//
// CumeDist is the Golang equivalent of cume_dist: () -> pyspark.sql.connect.column.Column
func CumeDist() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cume_dist"))
}

// DenseRank - Window function: returns the rank of rows within a window partition, without any gaps.
//
// The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
// sequence when there are ties. That is, if you were ranking a competition using dense_rank
// and had three people tie for second place, you would say that all three were in second
// place and that the next person came in third. Rank would give me sequential numbers, making
// the person that came in third place (after the ties) would register as coming in fifth.
//
// This is equivalent to the DENSE_RANK function in SQL.
//
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

// CountIf - Returns the number of `TRUE` values for the `col`.
//
// CountIf is the Golang equivalent of count_if: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CountIf(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("count_if", col))
}

// HistogramNumeric - Computes a histogram on numeric 'col' using nb bins.
// The return value is an array of (x,y) pairs representing the centers of the
// histogram's bins. As the value of 'nb' is increased, the histogram approximation
// gets finer-grained, but may yield artifacts around outliers. In practice, 20-40
// histogram bins appear to work well, with more bins being required for skewed or
// smaller datasets. Note that this function creates a histogram with non-uniform
// bin widths. It offers no guarantees in terms of the mean-squared-error of the
// histogram, but in practice is comparable to the histograms produced by the R/S-Plus
// statistical computing packages. Note: the output type of the 'x' field in the return value is
// propagated from the input value consumed in the aggregate function.
//
// HistogramNumeric is the Golang equivalent of histogram_numeric: (col: 'ColumnOrName', nBins: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func HistogramNumeric(col column.Column, nBins column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("histogram_numeric", col, nBins))
}

// Ntile - Window function: returns the ntile group id (from 1 to `n` inclusive)
// in an ordered window partition. For example, if `n` is 4, the first
// quarter of the rows will get value 1, the second quarter will get 2,
// the third quarter will get 3, and the last quarter will get 4.
//
// This is equivalent to the NTILE function in SQL.
//
// Ntile is the Golang equivalent of ntile: (n: int) -> pyspark.sql.connect.column.Column
func Ntile(n int64) column.Column {
	lit_n := Int64Lit(n)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ntile", lit_n))
}

// PercentRank - Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
//
// PercentRank is the Golang equivalent of percent_rank: () -> pyspark.sql.connect.column.Column
func PercentRank() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("percent_rank"))
}

// Rank - Window function: returns the rank of rows within a window partition.
//
// The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
// sequence when there are ties. That is, if you were ranking a competition using dense_rank
// and had three people tie for second place, you would say that all three were in second
// place and that the next person came in third. Rank would give me sequential numbers, making
// the person that came in third place (after the ties) would register as coming in fifth.
//
// This is equivalent to the RANK function in SQL.
//
// Rank is the Golang equivalent of rank: () -> pyspark.sql.connect.column.Column
func Rank() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rank"))
}

// RowNumber - Window function: returns a sequential number starting at 1 within a window partition.
//
// RowNumber is the Golang equivalent of row_number: () -> pyspark.sql.connect.column.Column
func RowNumber() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("row_number"))
}

// TODO: aggregate: (col: 'ColumnOrName', initialValue: 'ColumnOrName', merge: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], finish: Optional[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// TODO: reduce: (col: 'ColumnOrName', initialValue: 'ColumnOrName', merge: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], finish: Optional[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// Array - Creates a new array column.
//
// Array is the Golang equivalent of array: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func Array(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array", cols))
}

// TODO: array_append: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_contains: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// ArrayDistinct - Collection function: removes duplicate values from the array.
//
// ArrayDistinct is the Golang equivalent of array_distinct: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayDistinct(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_distinct", col))
}

// ArrayExcept - Collection function: returns an array of the elements in col1 but not in col2,
// without duplicates.
//
// ArrayExcept is the Golang equivalent of array_except: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayExcept(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_except", col1, col2))
}

// TODO: array_insert: (arr: 'ColumnOrName', pos: Union[ForwardRef('ColumnOrName'), int], value: Any) -> pyspark.sql.connect.column.Column

// ArrayIntersect - Collection function: returns an array of the elements in the intersection of col1 and col2,
// without duplicates.
//
// ArrayIntersect is the Golang equivalent of array_intersect: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayIntersect(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_intersect", col1, col2))
}

// ArrayCompact - Collection function: removes null values from the array.
//
// ArrayCompact is the Golang equivalent of array_compact: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayCompact(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_compact", col))
}

// ArrayJoin - Concatenates the elements of `column` using the `delimiter`. Null values are replaced with
// `null_replacement` if set, otherwise they are ignored.
//
// ArrayJoin is the Golang equivalent of array_join: (col: 'ColumnOrName', delimiter: str, null_replacement: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ArrayJoin(col column.Column, delimiter string, null_replacement string) column.Column {
	lit_delimiter := StringLit(delimiter)
	lit_null_replacement := StringLit(null_replacement)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_join", col, lit_delimiter, lit_null_replacement))
}

// ArrayMax - Collection function: returns the maximum value of the array.
//
// ArrayMax is the Golang equivalent of array_max: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayMax(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_max", col))
}

// ArrayMin - Collection function: returns the minimum value of the array.
//
// ArrayMin is the Golang equivalent of array_min: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayMin(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_min", col))
}

// ArraySize - Returns the total number of elements in the array. The function returns null for null input.
//
// ArraySize is the Golang equivalent of array_size: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraySize(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_size", col))
}

// Cardinality - Collection function: returns the length of the array or map stored in the column.
//
// Cardinality is the Golang equivalent of cardinality: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Cardinality(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("cardinality", col))
}

// TODO: array_position: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_prepend: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// TODO: array_remove: (col: 'ColumnOrName', element: Any) -> pyspark.sql.connect.column.Column

// ArrayRepeat - Collection function: creates an array containing a column repeated count times.
//
// ArrayRepeat is the Golang equivalent of array_repeat: (col: 'ColumnOrName', count: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func ArrayRepeat(col column.Column, count column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_repeat", col, count))
}

// TODO: array_sort: (col: 'ColumnOrName', comparator: Optional[Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]] = None) -> pyspark.sql.connect.column.Column

// ArrayUnion - Collection function: returns an array of the elements in the union of col1 and col2,
// without duplicates.
//
// ArrayUnion is the Golang equivalent of array_union: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArrayUnion(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("array_union", col1, col2))
}

// ArraysOverlap - Collection function: returns true if the arrays contain any common non-null element; if not,
// returns null if both the arrays are non-empty and any of them contains a null element; returns
// false otherwise.
//
// ArraysOverlap is the Golang equivalent of arrays_overlap: (a1: 'ColumnOrName', a2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraysOverlap(a1 column.Column, a2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("arrays_overlap", a1, a2))
}

// ArraysZip - Collection function: Returns a merged array of structs in which the N-th struct contains all
// N-th values of input arrays. If one of the arrays is shorter than others then
// resulting struct type value will be a `null` for missing elements.
//
// ArraysZip is the Golang equivalent of arrays_zip: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ArraysZip(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("arrays_zip", vals...))
}

// Concat - Concatenates multiple input columns together into a single column.
// The function works with strings, numeric, binary and compatible array columns.
//
// Concat is the Golang equivalent of concat: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Concat(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("concat", vals...))
}

// CreateMap - Creates a new map column.
//
// CreateMap is the Golang equivalent of create_map: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func CreateMap(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("create_map", cols))
}

// TODO: element_at: (col: 'ColumnOrName', extraction: Any) -> pyspark.sql.connect.column.Column

// TryElementAt - (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will
// throw an error. If index < 0, accesses elements from the last to the first. The function
// always returns NULL if the index exceeds the length of the array.
//
// (map, key) - Returns value for given key. The function always returns NULL if the key is not
// contained in the map.
//
// TryElementAt is the Golang equivalent of try_element_at: (col: 'ColumnOrName', extraction: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryElementAt(col column.Column, extraction column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_element_at", col, extraction))
}

// TODO: exists: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// Explode - Returns a new row for each element in the given array or map.
// Uses the default column name `col` for elements in the array and
// `key` and `value` for elements in the map unless specified otherwise.
//
// Explode is the Golang equivalent of explode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Explode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("explode", col))
}

// ExplodeOuter - Returns a new row for each element in the given array or map.
// Unlike explode, if the array/map is null or empty then null is produced.
// Uses the default column name `col` for elements in the array and
// `key` and `value` for elements in the map unless specified otherwise.
//
// ExplodeOuter is the Golang equivalent of explode_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ExplodeOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("explode_outer", col))
}

// TODO: filter: (col: 'ColumnOrName', f: Union[Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column], Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]]) -> pyspark.sql.connect.column.Column

// Flatten - Collection function: creates a single array from an array of arrays.
// If a structure of nested arrays is deeper than two levels,
// only one level of nesting is removed.
//
// Flatten is the Golang equivalent of flatten: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Flatten(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("flatten", col))
}

// TODO: forall: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// TODO: from_csv: (col: 'ColumnOrName', schema: Union[pyspark.sql.connect.column.Column, str], options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: from_json: (col: 'ColumnOrName', schema: Union[pyspark.sql.types.ArrayType, pyspark.sql.types.StructType, pyspark.sql.connect.column.Column, str], options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// Get - Collection function: Returns element of array at given (0-based) index.
// If the index points outside of the array boundaries, then this function
// returns NULL.
//
// Get is the Golang equivalent of get: (col: 'ColumnOrName', index: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Get(col column.Column, index column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("get", col, index))
}

// GetJsonObject - Extracts json object from a json string based on json `path` specified, and returns json string
// of the extracted json object. It will return null if the input json string is invalid.
//
// GetJsonObject is the Golang equivalent of get_json_object: (col: 'ColumnOrName', path: str) -> pyspark.sql.connect.column.Column
func GetJsonObject(col column.Column, path string) column.Column {
	lit_path := StringLit(path)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("get_json_object", col, lit_path))
}

// JsonArrayLength - Returns the number of elements in the outermost JSON array. `NULL` is returned in case of
// any other valid JSON string, `NULL` or an invalid JSON.
//
// JsonArrayLength is the Golang equivalent of json_array_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JsonArrayLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_array_length", col))
}

// JsonObjectKeys - Returns all the keys of the outermost JSON object as an array. If a valid JSON object is
// given, all the keys of the outermost object will be returned as an array. If it is any
// other valid JSON string, an invalid JSON string or an empty string, the function returns null.
//
// JsonObjectKeys is the Golang equivalent of json_object_keys: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JsonObjectKeys(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_object_keys", col))
}

// Inline - Explodes an array of structs into a table.
//
// Inline is the Golang equivalent of inline: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Inline(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("inline", col))
}

// InlineOuter - Explodes an array of structs into a table.
// Unlike inline, if the array is null or empty then null is produced for each nested column.
//
// InlineOuter is the Golang equivalent of inline_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func InlineOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("inline_outer", col))
}

// JsonTuple - Creates a new row for a json column according to the given field names.
//
// JsonTuple is the Golang equivalent of json_tuple: (col: 'ColumnOrName', *fields: str) -> pyspark.sql.connect.column.Column
func JsonTuple(col column.Column, fields string) column.Column {
	lit_fields := StringLit(fields)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("json_tuple", col, lit_fields))
}

// MapConcat - Returns the union of all the given maps.
//
// MapConcat is the Golang equivalent of map_concat: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func MapConcat(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_concat", cols))
}

// TODO: map_contains_key: (col: 'ColumnOrName', value: Any) -> pyspark.sql.connect.column.Column

// MapEntries - Collection function: Returns an unordered array of all entries in the given map.
//
// MapEntries is the Golang equivalent of map_entries: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapEntries(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_entries", col))
}

// TODO: map_filter: (col: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// MapFromArrays - Creates a new map from two arrays.
//
// MapFromArrays is the Golang equivalent of map_from_arrays: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapFromArrays(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_from_arrays", col1, col2))
}

// MapFromEntries - Collection function: Converts an array of entries (key value struct types) to a map
// of values.
//
// MapFromEntries is the Golang equivalent of map_from_entries: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapFromEntries(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_from_entries", col))
}

// MapKeys - Collection function: Returns an unordered array containing the keys of the map.
//
// MapKeys is the Golang equivalent of map_keys: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapKeys(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_keys", col))
}

// MapValues - Collection function: Returns an unordered array containing the values of the map.
//
// MapValues is the Golang equivalent of map_values: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MapValues(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("map_values", col))
}

// TODO: map_zip_with: (col1: 'ColumnOrName', col2: 'ColumnOrName', f: Callable[[pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column, pyspark.sql.connect.column.Column], pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// StrToMap - Creates a map after splitting the text into key/value pairs using delimiters.
// Both `pairDelim` and `keyValueDelim` are treated as regular expressions.
//
// StrToMap is the Golang equivalent of str_to_map: (text: 'ColumnOrName', pairDelim: Optional[ForwardRef('ColumnOrName')] = None, keyValueDelim: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func StrToMap(text column.Column, pairDelim column.Column, keyValueDelim column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("str_to_map", text, pairDelim, keyValueDelim))
}

// Posexplode - Returns a new row for each element with position in the given array or map.
// Uses the default column name `pos` for position, and `col` for elements in the
// array and `key` and `value` for elements in the map unless specified otherwise.
//
// Posexplode is the Golang equivalent of posexplode: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Posexplode(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("posexplode", col))
}

// PosexplodeOuter - Returns a new row for each element with position in the given array or map.
// Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced.
// Uses the default column name `pos` for position, and `col` for elements in the
// array and `key` and `value` for elements in the map unless specified otherwise.
//
// PosexplodeOuter is the Golang equivalent of posexplode_outer: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func PosexplodeOuter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("posexplode_outer", col))
}

// Reverse - Collection function: returns a reversed string or an array with reverse order of elements.
//
// Reverse is the Golang equivalent of reverse: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Reverse(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("reverse", col))
}

// Sequence - Generate a sequence of integers from `start` to `stop`, incrementing by `step`.
// If `step` is not set, incrementing by 1 if `start` is less than or equal to `stop`,
// otherwise -1.
//
// Sequence is the Golang equivalent of sequence: (start: 'ColumnOrName', stop: 'ColumnOrName', step: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Sequence(start column.Column, stop column.Column, step column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sequence", start, stop, step))
}

// TODO: schema_of_csv: (csv: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// TODO: schema_of_json: (json: 'ColumnOrName', options: Optional[Dict[str, str]] = None) -> pyspark.sql.connect.column.Column

// Shuffle - Collection function: Generates a random permutation of the given array.
//
// Shuffle is the Golang equivalent of shuffle: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Shuffle(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("shuffle", col))
}

// Size - Collection function: returns the length of the array or map stored in the column.
//
// Size is the Golang equivalent of size: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Size(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("size", col))
}

// Slice - Collection function: returns an array containing all the elements in `x` from index `start`
// (array indices start at 1, or from the end if `start` is negative) with the specified `length`.
//
// Slice is the Golang equivalent of slice: (col: 'ColumnOrName', start: Union[ForwardRef('ColumnOrName'), int], length: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Slice(col column.Column, start column.Column, length column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("slice", col, start, length))
}

// TODO: sort_array: (col: 'ColumnOrName', asc: bool = True) -> pyspark.sql.connect.column.Column

// Struct - Creates a new struct column.
//
// Struct is the Golang equivalent of struct: (*cols: Union[ForwardRef('ColumnOrName'), List[ForwardRef('ColumnOrName')], Tuple[ForwardRef('ColumnOrName'), ...]]) -> pyspark.sql.connect.column.Column
func Struct(cols column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("struct", cols))
}

// NamedStruct - Creates a struct with the given field names and values.
//
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

// Upper - Converts a string expression to upper case.
//
// Upper is the Golang equivalent of upper: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Upper(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("upper", col))
}

// Lower - Converts a string expression to lower case.
//
// Lower is the Golang equivalent of lower: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Lower(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lower", col))
}

// Ascii - Computes the numeric value of the first character of the string column.
//
// Ascii is the Golang equivalent of ascii: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ascii(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ascii", col))
}

// Base64 - Computes the BASE64 encoding of a binary column and returns it as a string column.
//
// Base64 is the Golang equivalent of base64: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Base64(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("base64", col))
}

// Unbase64 - Decodes a BASE64 encoded string column and returns it as a binary column.
//
// Unbase64 is the Golang equivalent of unbase64: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Unbase64(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unbase64", col))
}

// Ltrim - Trim the spaces from left end for the specified string value.
//
// Ltrim is the Golang equivalent of ltrim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ltrim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ltrim", col))
}

// Rtrim - Trim the spaces from right end for the specified string value.
//
// Rtrim is the Golang equivalent of rtrim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rtrim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rtrim", col))
}

// Trim - Trim the spaces from both ends for the specified string column.
//
// Trim is the Golang equivalent of trim: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Trim(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("trim", col))
}

// ConcatWs - Concatenates multiple input string columns together into a single string column,
// using the given separator.
//
// ConcatWs is the Golang equivalent of concat_ws: (sep: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ConcatWs(sep string, cols ...column.Column) column.Column {
	lit_sep := StringLit(sep)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_sep)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("concat_ws", vals...))
}

// Decode - Computes the first argument into a string from a binary using the provided character set
// (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
//
// Decode is the Golang equivalent of decode: (col: 'ColumnOrName', charset: str) -> pyspark.sql.connect.column.Column
func Decode(col column.Column, charset string) column.Column {
	lit_charset := StringLit(charset)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("decode", col, lit_charset))
}

// Encode - Computes the first argument into a binary from a string using the provided character set
// (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
//
// Encode is the Golang equivalent of encode: (col: 'ColumnOrName', charset: str) -> pyspark.sql.connect.column.Column
func Encode(col column.Column, charset string) column.Column {
	lit_charset := StringLit(charset)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("encode", col, lit_charset))
}

// FormatNumber - Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places
// with HALF_EVEN round mode, and returns the result as a string.
//
// FormatNumber is the Golang equivalent of format_number: (col: 'ColumnOrName', d: int) -> pyspark.sql.connect.column.Column
func FormatNumber(col column.Column, d int64) column.Column {
	lit_d := Int64Lit(d)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("format_number", col, lit_d))
}

// FormatString - Formats the arguments in printf-style and returns the result as a string column.
//
// FormatString is the Golang equivalent of format_string: (format: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FormatString(format string, cols ...column.Column) column.Column {
	lit_format := StringLit(format)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_format)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("format_string", vals...))
}

// Instr - Locate the position of the first occurrence of substr column in the given string.
// Returns null if either of the arguments are null.
//
// Instr is the Golang equivalent of instr: (str: 'ColumnOrName', substr: str) -> pyspark.sql.connect.column.Column
func Instr(str column.Column, substr string) column.Column {
	lit_substr := StringLit(substr)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("instr", str, lit_substr))
}

// Overlay - Overlay the specified portion of `src` with `replace`,
// starting from byte position `pos` of `src` and proceeding for `len` bytes.
//
// Overlay is the Golang equivalent of overlay: (src: 'ColumnOrName', replace: 'ColumnOrName', pos: Union[ForwardRef('ColumnOrName'), int], len: Union[ForwardRef('ColumnOrName'), int] = -1) -> pyspark.sql.connect.column.Column
func Overlay(src column.Column, replace column.Column, pos column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("overlay", src, replace, pos, len))
}

// Sentences - Splits a string into arrays of sentences, where each sentence is an array of words.
// The 'language' and 'country' arguments are optional, and if omitted, the default locale is used.
//
// Sentences is the Golang equivalent of sentences: (string: 'ColumnOrName', language: Optional[ForwardRef('ColumnOrName')] = None, country: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Sentences(string column.Column, language column.Column, country column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sentences", string, language, country))
}

// Substring - Substring starts at `pos` and is of length `len` when str is String type or
// returns the slice of byte array that starts at `pos` in byte and is of length `len`
// when str is Binary type.
//
// Substring is the Golang equivalent of substring: (str: 'ColumnOrName', pos: int, len: int) -> pyspark.sql.connect.column.Column
func Substring(str column.Column, pos int64, len int64) column.Column {
	lit_pos := Int64Lit(pos)
	lit_len := Int64Lit(len)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substring", str, lit_pos, lit_len))
}

// SubstringIndex - Returns the substring from string str before count occurrences of the delimiter delim.
// If count is positive, everything the left of the final delimiter (counting from left) is
// returned. If count is negative, every to the right of the final delimiter (counting from the
// right) is returned. substring_index performs a case-sensitive match when searching for delim.
//
// SubstringIndex is the Golang equivalent of substring_index: (str: 'ColumnOrName', delim: str, count: int) -> pyspark.sql.connect.column.Column
func SubstringIndex(str column.Column, delim string, count int64) column.Column {
	lit_delim := StringLit(delim)
	lit_count := Int64Lit(count)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substring_index", str, lit_delim, lit_count))
}

// Levenshtein - Computes the Levenshtein distance of the two given strings.
//
// Levenshtein is the Golang equivalent of levenshtein: (left: 'ColumnOrName', right: 'ColumnOrName', threshold: Optional[int] = None) -> pyspark.sql.connect.column.Column
func Levenshtein(left column.Column, right column.Column, threshold int64) column.Column {
	lit_threshold := Int64Lit(threshold)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("levenshtein", left, right, lit_threshold))
}

// Locate - Locate the position of the first occurrence of substr in a string column, after position pos.
//
// Locate is the Golang equivalent of locate: (substr: str, str: 'ColumnOrName', pos: int = 1) -> pyspark.sql.connect.column.Column
func Locate(substr string, str column.Column, pos int64) column.Column {
	lit_substr := StringLit(substr)
	lit_pos := Int64Lit(pos)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("locate", lit_substr, str, lit_pos))
}

// Lpad - Left-pad the string column to width `len` with `pad`.
//
// Lpad is the Golang equivalent of lpad: (col: 'ColumnOrName', len: int, pad: str) -> pyspark.sql.connect.column.Column
func Lpad(col column.Column, len int64, pad string) column.Column {
	lit_len := Int64Lit(len)
	lit_pad := StringLit(pad)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lpad", col, lit_len, lit_pad))
}

// Rpad - Right-pad the string column to width `len` with `pad`.
//
// Rpad is the Golang equivalent of rpad: (col: 'ColumnOrName', len: int, pad: str) -> pyspark.sql.connect.column.Column
func Rpad(col column.Column, len int64, pad string) column.Column {
	lit_len := Int64Lit(len)
	lit_pad := StringLit(pad)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rpad", col, lit_len, lit_pad))
}

// Repeat - Repeats a string column n times, and returns it as a new string column.
//
// Repeat is the Golang equivalent of repeat: (col: 'ColumnOrName', n: int) -> pyspark.sql.connect.column.Column
func Repeat(col column.Column, n int64) column.Column {
	lit_n := Int64Lit(n)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("repeat", col, lit_n))
}

// Split - Splits str around matches of the given pattern.
//
// Split is the Golang equivalent of split: (str: 'ColumnOrName', pattern: str, limit: int = -1) -> pyspark.sql.connect.column.Column
func Split(str column.Column, pattern string, limit int64) column.Column {
	lit_pattern := StringLit(pattern)
	lit_limit := Int64Lit(limit)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("split", str, lit_pattern, lit_limit))
}

// Rlike - Returns true if `str` matches the Java regex `regexp`, or false otherwise.
//
// Rlike is the Golang equivalent of rlike: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Rlike(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("rlike", str, regexp))
}

// Regexp - Returns true if `str` matches the Java regex `regexp`, or false otherwise.
//
// Regexp is the Golang equivalent of regexp: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Regexp(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp", str, regexp))
}

// RegexpLike - Returns true if `str` matches the Java regex `regexp`, or false otherwise.
//
// RegexpLike is the Golang equivalent of regexp_like: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpLike(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_like", str, regexp))
}

// RegexpCount - Returns a count of the number of times that the Java regex pattern `regexp` is matched
// in the string `str`.
//
// RegexpCount is the Golang equivalent of regexp_count: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpCount(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_count", str, regexp))
}

// RegexpExtract - Extract a specific group matched by the Java regex `regexp`, from the specified string column.
// If the regex did not match, or the specified group did not match, an empty string is returned.
//
// RegexpExtract is the Golang equivalent of regexp_extract: (str: 'ColumnOrName', pattern: str, idx: int) -> pyspark.sql.connect.column.Column
func RegexpExtract(str column.Column, pattern string, idx int64) column.Column {
	lit_pattern := StringLit(pattern)
	lit_idx := Int64Lit(idx)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_extract", str, lit_pattern, lit_idx))
}

// TODO: regexp_extract_all: (str: 'ColumnOrName', regexp: 'ColumnOrName', idx: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: regexp_replace: (string: 'ColumnOrName', pattern: Union[str, pyspark.sql.connect.column.Column], replacement: Union[str, pyspark.sql.connect.column.Column]) -> pyspark.sql.connect.column.Column

// RegexpSubstr - Returns the substring that matches the Java regex `regexp` within the string `str`.
// If the regular expression is not found, the result is null.
//
// RegexpSubstr is the Golang equivalent of regexp_substr: (str: 'ColumnOrName', regexp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func RegexpSubstr(str column.Column, regexp column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("regexp_substr", str, regexp))
}

// TODO: regexp_instr: (str: 'ColumnOrName', regexp: 'ColumnOrName', idx: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// Initcap - Translate the first letter of each word to upper case in the sentence.
//
// Initcap is the Golang equivalent of initcap: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Initcap(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("initcap", col))
}

// Soundex - Returns the SoundEx encoding for a string
//
// Soundex is the Golang equivalent of soundex: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Soundex(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("soundex", col))
}

// Length - Computes the character length of string data or number of bytes of binary data.
// The length of character data includes the trailing spaces. The length of binary data
// includes binary zeros.
//
// Length is the Golang equivalent of length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Length(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("length", col))
}

// OctetLength - Calculates the byte length for the specified string column.
//
// OctetLength is the Golang equivalent of octet_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func OctetLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("octet_length", col))
}

// BitLength - Calculates the bit length for the specified string column.
//
// BitLength is the Golang equivalent of bit_length: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitLength(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bit_length", col))
}

// Translate - A function translate any character in the `srcCol` by a character in `matching`.
// The characters in `replace` is corresponding to the characters in `matching`.
// Translation will happen whenever any character in the string is matching with the character
// in the `matching`.
//
// Translate is the Golang equivalent of translate: (srcCol: 'ColumnOrName', matching: str, replace: str) -> pyspark.sql.connect.column.Column
func Translate(srcCol column.Column, matching string, replace string) column.Column {
	lit_matching := StringLit(matching)
	lit_replace := StringLit(replace)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("translate", srcCol, lit_matching, lit_replace))
}

// ToBinary - Converts the input `col` to a binary value based on the supplied `format`.
// The `format` can be a case-insensitive string literal of "hex", "utf-8", "utf8",
// or "base64". By default, the binary format for conversion is "hex" if
// `format` is omitted. The function returns NULL if at least one of the
// input parameters is NULL.
//
// ToBinary is the Golang equivalent of to_binary: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToBinary(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_binary", col, format))
}

// ToChar - Convert `col` to a string based on the `format`.
// Throws an exception if the conversion fails. The format can consist of the following
// characters, case insensitive:
// '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
// format string matches a sequence of digits in the input value, generating a result
// string of the same length as the corresponding sequence in the format string.
// The result string is left-padded with zeros if the 0/9 sequence comprises more digits
// than the matching part of the decimal value, starts with 0, and is before the decimal
// point. Otherwise, it is padded with spaces.
// '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
// ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
// There must be a 0 or 9 to the left and right of each grouping separator.
// '$': Specifies the location of the $ currency sign. This character may only be specified once.
// 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at
// the beginning or end of the format string). Note that 'S' prints '+' for positive
// values but 'MI' prints a space.
// 'PR': Only allowed at the end of the format string; specifies that the result string
// will be wrapped by angle brackets if the input value is negative.
//
// ToChar is the Golang equivalent of to_char: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToChar(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_char", col, format))
}

// ToVarchar - Convert `col` to a string based on the `format`.
// Throws an exception if the conversion fails. The format can consist of the following
// characters, case insensitive:
// '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
// format string matches a sequence of digits in the input value, generating a result
// string of the same length as the corresponding sequence in the format string.
// The result string is left-padded with zeros if the 0/9 sequence comprises more digits
// than the matching part of the decimal value, starts with 0, and is before the decimal
// point. Otherwise, it is padded with spaces.
// '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
// ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
// There must be a 0 or 9 to the left and right of each grouping separator.
// '$': Specifies the location of the $ currency sign. This character may only be specified once.
// 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at
// the beginning or end of the format string). Note that 'S' prints '+' for positive
// values but 'MI' prints a space.
// 'PR': Only allowed at the end of the format string; specifies that the result string
// will be wrapped by angle brackets if the input value is negative.
//
// ToVarchar is the Golang equivalent of to_varchar: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToVarchar(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_varchar", col, format))
}

// ToNumber - Convert string 'col' to a number based on the string format 'format'.
// Throws an exception if the conversion fails. The format can consist of the following
// characters, case insensitive:
// '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the
// format string matches a sequence of digits in the input string. If the 0/9
// sequence starts with 0 and is before the decimal point, it can only match a digit
// sequence of the same size. Otherwise, if the sequence starts with 9 or is after
// the decimal point, it can match a digit sequence that has the same or smaller size.
// '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
// ',' or 'G': Specifies the position of the grouping (thousands) separator (,).
// There must be a 0 or 9 to the left and right of each grouping separator.
// 'col' must match the grouping separator relevant for the size of the number.
// '$': Specifies the location of the $ currency sign. This character may only be
// specified once.
// 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed
// once at the beginning or end of the format string). Note that 'S' allows '-'
// but 'MI' does not.
// 'PR': Only allowed at the end of the format string; specifies that 'col' indicates a
// negative number with wrapping angled brackets.
//
// ToNumber is the Golang equivalent of to_number: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToNumber(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_number", col, format))
}

// Replace - Replaces all occurrences of `search` with `replace`.
//
// Replace is the Golang equivalent of replace: (src: 'ColumnOrName', search: 'ColumnOrName', replace: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Replace(src column.Column, search column.Column, replace column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("replace", src, search, replace))
}

// SplitPart - Splits `str` by delimiter and return requested part of the split (1-based).
// If any input is null, returns null. if `partNum` is out of range of split parts,
// returns empty string. If `partNum` is 0, throws an error. If `partNum` is negative,
// the parts are counted backward from the end of the string.
// If the `delimiter` is an empty string, the `str` is not split.
//
// SplitPart is the Golang equivalent of split_part: (src: 'ColumnOrName', delimiter: 'ColumnOrName', partNum: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func SplitPart(src column.Column, delimiter column.Column, partNum column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("split_part", src, delimiter, partNum))
}

// Substr - Returns the substring of `str` that starts at `pos` and is of length `len`,
// or the slice of byte array that starts at `pos` and is of length `len`.
//
// Substr is the Golang equivalent of substr: (str: 'ColumnOrName', pos: 'ColumnOrName', len: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Substr(str column.Column, pos column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("substr", str, pos, len))
}

// ParseUrl - Extracts a part from a URL.
//
// ParseUrl is the Golang equivalent of parse_url: (url: 'ColumnOrName', partToExtract: 'ColumnOrName', key: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ParseUrl(url column.Column, partToExtract column.Column, key column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("parse_url", url, partToExtract, key))
}

// Printf - Formats the arguments in printf-style and returns the result as a string column.
//
// Printf is the Golang equivalent of printf: (format: 'ColumnOrName', *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Printf(format column.Column, cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, format)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("printf", vals...))
}

// UrlDecode - Decodes a `str` in 'application/x-www-form-urlencoded' format
// using a specific encoding scheme.
//
// UrlDecode is the Golang equivalent of url_decode: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UrlDecode(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("url_decode", str))
}

// UrlEncode - Translates a string into 'application/x-www-form-urlencoded' format
// using a specific encoding scheme.
//
// UrlEncode is the Golang equivalent of url_encode: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UrlEncode(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("url_encode", str))
}

// Position - Returns the position of the first occurrence of `substr` in `str` after position `start`.
// The given `start` and return value are 1-based.
//
// Position is the Golang equivalent of position: (substr: 'ColumnOrName', str: 'ColumnOrName', start: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Position(substr column.Column, str column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("position", substr, str, start))
}

// Endswith - Returns a boolean. The value is True if str ends with suffix.
// Returns NULL if either input expression is NULL. Otherwise, returns False.
// Both str or suffix must be of STRING or BINARY type.
//
// Endswith is the Golang equivalent of endswith: (str: 'ColumnOrName', suffix: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Endswith(str column.Column, suffix column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("endswith", str, suffix))
}

// Startswith - Returns a boolean. The value is True if str starts with prefix.
// Returns NULL if either input expression is NULL. Otherwise, returns False.
// Both str or prefix must be of STRING or BINARY type.
//
// Startswith is the Golang equivalent of startswith: (str: 'ColumnOrName', prefix: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Startswith(str column.Column, prefix column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("startswith", str, prefix))
}

// Char - Returns the ASCII character having the binary equivalent to `col`. If col is larger than 256 the
// result is equivalent to char(col % 256)
//
// Char is the Golang equivalent of char: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Char(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("char", col))
}

// TryToBinary - This is a special version of `to_binary` that performs the same operation, but returns a NULL
// value instead of raising an error if the conversion cannot be performed.
//
// TryToBinary is the Golang equivalent of try_to_binary: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryToBinary(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_binary", col, format))
}

// TryToNumber - Convert string 'col' to a number based on the string format `format`. Returns NULL if the
// string 'col' does not match the expected format. The format follows the same semantics as the
// to_number function.
//
// TryToNumber is the Golang equivalent of try_to_number: (col: 'ColumnOrName', format: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TryToNumber(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_number", col, format))
}

// Btrim - Remove the leading and trailing `trim` characters from `str`.
//
// Btrim is the Golang equivalent of btrim: (str: 'ColumnOrName', trim: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Btrim(str column.Column, trim column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("btrim", str, trim))
}

// CharLength - Returns the character length of string data or number of bytes of binary data.
// The length of string data includes the trailing spaces.
// The length of binary data includes binary zeros.
//
// CharLength is the Golang equivalent of char_length: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CharLength(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("char_length", str))
}

// CharacterLength - Returns the character length of string data or number of bytes of binary data.
// The length of string data includes the trailing spaces.
// The length of binary data includes binary zeros.
//
// CharacterLength is the Golang equivalent of character_length: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CharacterLength(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("character_length", str))
}

// Contains - Returns a boolean. The value is True if right is found inside left.
// Returns NULL if either input expression is NULL. Otherwise, returns False.
// Both left or right must be of STRING or BINARY type.
//
// Contains is the Golang equivalent of contains: (left: 'ColumnOrName', right: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Contains(left column.Column, right column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("contains", left, right))
}

// Elt - Returns the `n`-th input, e.g., returns `input2` when `n` is 2.
// The function returns NULL if the index exceeds the length of the array
// and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true,
// it throws ArrayIndexOutOfBoundsException for invalid indices.
//
// Elt is the Golang equivalent of elt: (*inputs: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Elt(inputs ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, inputs...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("elt", vals...))
}

// FindInSet - Returns the index (1-based) of the given string (`str`) in the comma-delimited
// list (`strArray`). Returns 0, if the string was not found or if the given string (`str`)
// contains a comma.
//
// FindInSet is the Golang equivalent of find_in_set: (str: 'ColumnOrName', str_array: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FindInSet(str column.Column, str_array column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("find_in_set", str, str_array))
}

// TODO: like: (str: 'ColumnOrName', pattern: 'ColumnOrName', escapeChar: Optional[ForwardRef('Column')] = None) -> pyspark.sql.connect.column.Column

// TODO: ilike: (str: 'ColumnOrName', pattern: 'ColumnOrName', escapeChar: Optional[ForwardRef('Column')] = None) -> pyspark.sql.connect.column.Column

// Lcase - Returns `str` with all characters changed to lowercase.
//
// Lcase is the Golang equivalent of lcase: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Lcase(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("lcase", str))
}

// Ucase - Returns `str` with all characters changed to uppercase.
//
// Ucase is the Golang equivalent of ucase: (str: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ucase(str column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ucase", str))
}

// Left - Returns the leftmost `len`(`len` can be string type) characters from the string `str`,
// if `len` is less or equal than 0 the result is an empty string.
//
// Left is the Golang equivalent of left: (str: 'ColumnOrName', len: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Left(str column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("left", str, len))
}

// Right - Returns the rightmost `len`(`len` can be string type) characters from the string `str`,
// if `len` is less or equal than 0 the result is an empty string.
//
// Right is the Golang equivalent of right: (str: 'ColumnOrName', len: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Right(str column.Column, len column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("right", str, len))
}

// Mask - Masks the given string value. This can be useful for creating copies of tables with sensitive
// information removed.
//
// Mask is the Golang equivalent of mask: (col: 'ColumnOrName', upperChar: Optional[ForwardRef('ColumnOrName')] = None, lowerChar: Optional[ForwardRef('ColumnOrName')] = None, digitChar: Optional[ForwardRef('ColumnOrName')] = None, otherChar: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func Mask(col column.Column, upperChar column.Column, lowerChar column.Column,
	digitChar column.Column, otherChar column.Column,
) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("mask", col, upperChar, lowerChar, digitChar, otherChar))
}

// Curdate - Returns the current date at the start of query evaluation as a :class:`DateType` column.
// All calls of current_date within the same query return the same value.
//
// Curdate is the Golang equivalent of curdate: () -> pyspark.sql.connect.column.Column
func Curdate() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("curdate"))
}

// CurrentDate - Returns the current date at the start of query evaluation as a :class:`DateType` column.
// All calls of current_date within the same query return the same value.
//
// CurrentDate is the Golang equivalent of current_date: () -> pyspark.sql.connect.column.Column
func CurrentDate() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_date"))
}

// CurrentTimestamp - Returns the current timestamp at the start of query evaluation as a :class:`TimestampType`
// column. All calls of current_timestamp within the same query return the same value.
//
// CurrentTimestamp is the Golang equivalent of current_timestamp: () -> pyspark.sql.connect.column.Column
func CurrentTimestamp() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_timestamp"))
}

// Now - Returns the current timestamp at the start of query evaluation.
//
// Now is the Golang equivalent of now: () -> pyspark.sql.connect.column.Column
func Now() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("now"))
}

// CurrentTimezone - Returns the current session local timezone.
//
// CurrentTimezone is the Golang equivalent of current_timezone: () -> pyspark.sql.connect.column.Column
func CurrentTimezone() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_timezone"))
}

// Localtimestamp - Returns the current timestamp without time zone at the start of query evaluation
// as a timestamp without time zone column. All calls of localtimestamp within the
// same query return the same value.
//
// Localtimestamp is the Golang equivalent of localtimestamp: () -> pyspark.sql.connect.column.Column
func Localtimestamp() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("localtimestamp"))
}

// DateFormat - Converts a date/timestamp/string to a value of string in the format specified by the date
// format given by the second argument.
//
// A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
// pattern letters of `datetime pattern`_. can be used.
//
// DateFormat is the Golang equivalent of date_format: (date: 'ColumnOrName', format: str) -> pyspark.sql.connect.column.Column
func DateFormat(date column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_format", date, lit_format))
}

// Year - Extract the year of a given date/timestamp as integer.
//
// Year is the Golang equivalent of year: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Year(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("year", col))
}

// Quarter - Extract the quarter of a given date/timestamp as integer.
//
// Quarter is the Golang equivalent of quarter: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Quarter(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("quarter", col))
}

// Month - Extract the month of a given date/timestamp as integer.
//
// Month is the Golang equivalent of month: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Month(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("month", col))
}

// Dayofweek - Extract the day of the week of a given date/timestamp as integer.
// Ranges from 1 for a Sunday through to 7 for a Saturday
//
// Dayofweek is the Golang equivalent of dayofweek: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofweek(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofweek", col))
}

// Dayofmonth - Extract the day of the month of a given date/timestamp as integer.
//
// Dayofmonth is the Golang equivalent of dayofmonth: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofmonth(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofmonth", col))
}

// Day - Extract the day of the month of a given date/timestamp as integer.
//
// Day is the Golang equivalent of day: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Day(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("day", col))
}

// Dayofyear - Extract the day of the year of a given date/timestamp as integer.
//
// Dayofyear is the Golang equivalent of dayofyear: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Dayofyear(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dayofyear", col))
}

// Hour - Extract the hours of a given timestamp as integer.
//
// Hour is the Golang equivalent of hour: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hour(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hour", col))
}

// Minute - Extract the minutes of a given timestamp as integer.
//
// Minute is the Golang equivalent of minute: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Minute(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("minute", col))
}

// Second - Extract the seconds of a given date as integer.
//
// Second is the Golang equivalent of second: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Second(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("second", col))
}

// Weekofyear - Extract the week number of a given date as integer.
// A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
// as defined by ISO 8601
//
// Weekofyear is the Golang equivalent of weekofyear: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Weekofyear(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("weekofyear", col))
}

// Weekday - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
//
// Weekday is the Golang equivalent of weekday: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Weekday(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("weekday", col))
}

// Extract - Extracts a part of the date/timestamp or interval source.
//
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

// MakeDate - Returns a column with a date built from the year, month and day columns.
//
// MakeDate is the Golang equivalent of make_date: (year: 'ColumnOrName', month: 'ColumnOrName', day: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MakeDate(year column.Column, month column.Column, day column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_date", year, month, day))
}

// DateAdd - Returns the date that is `days` days after `start`. If `days` is a negative value
// then these amount of days will be deducted from `start`.
//
// DateAdd is the Golang equivalent of date_add: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func DateAdd(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_add", start, days))
}

// Dateadd - Returns the date that is `days` days after `start`. If `days` is a negative value
// then these amount of days will be deducted from `start`.
//
// Dateadd is the Golang equivalent of dateadd: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func Dateadd(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("dateadd", start, days))
}

// DateSub - Returns the date that is `days` days before `start`. If `days` is a negative value
// then these amount of days will be added to `start`.
//
// DateSub is the Golang equivalent of date_sub: (start: 'ColumnOrName', days: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func DateSub(start column.Column, days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_sub", start, days))
}

// Datediff - Returns the number of days from `start` to `end`.
//
// Datediff is the Golang equivalent of datediff: (end: 'ColumnOrName', start: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Datediff(end column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("datediff", end, start))
}

// DateDiff - Returns the number of days from `start` to `end`.
//
// DateDiff is the Golang equivalent of date_diff: (end: 'ColumnOrName', start: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateDiff(end column.Column, start column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_diff", end, start))
}

// DateFromUnixDate - Create date from the number of `days` since 1970-01-01.
//
// DateFromUnixDate is the Golang equivalent of date_from_unix_date: (days: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateFromUnixDate(days column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_from_unix_date", days))
}

// AddMonths - Returns the date that is `months` months after `start`. If `months` is a negative value
// then these amount of months will be deducted from the `start`.
//
// AddMonths is the Golang equivalent of add_months: (start: 'ColumnOrName', months: Union[ForwardRef('ColumnOrName'), int]) -> pyspark.sql.connect.column.Column
func AddMonths(start column.Column, months column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("add_months", start, months))
}

// TODO: months_between: (date1: 'ColumnOrName', date2: 'ColumnOrName', roundOff: bool = True) -> pyspark.sql.connect.column.Column

// ToDate - Converts a :class:`~pyspark.sql.Column` into :class:`pyspark.sql.types.DateType`
// using the optionally specified format. Specify formats according to `datetime pattern`_.
// By default, it follows casting rules to :class:`pyspark.sql.types.DateType` if the format
// is omitted. Equivalent to “col.cast("date")“.
//
// ToDate is the Golang equivalent of to_date: (col: 'ColumnOrName', format: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ToDate(col column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_date", col, lit_format))
}

// UnixDate - Returns the number of days since 1970-01-01.
//
// UnixDate is the Golang equivalent of unix_date: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixDate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_date", col))
}

// UnixMicros - Returns the number of microseconds since 1970-01-01 00:00:00 UTC.
//
// UnixMicros is the Golang equivalent of unix_micros: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixMicros(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_micros", col))
}

// UnixMillis - Returns the number of milliseconds since 1970-01-01 00:00:00 UTC.
// Truncates higher levels of precision.
//
// UnixMillis is the Golang equivalent of unix_millis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixMillis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_millis", col))
}

// UnixSeconds - Returns the number of seconds since 1970-01-01 00:00:00 UTC.
// Truncates higher levels of precision.
//
// UnixSeconds is the Golang equivalent of unix_seconds: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func UnixSeconds(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_seconds", col))
}

// ToTimestamp - Converts a :class:`~pyspark.sql.Column` into :class:`pyspark.sql.types.TimestampType`
// using the optionally specified format. Specify formats according to `datetime pattern`_.
// By default, it follows casting rules to :class:`pyspark.sql.types.TimestampType` if the format
// is omitted. Equivalent to “col.cast("timestamp")“.
//
// ToTimestamp is the Golang equivalent of to_timestamp: (col: 'ColumnOrName', format: Optional[str] = None) -> pyspark.sql.connect.column.Column
func ToTimestamp(col column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp", col, lit_format))
}

// TryToTimestamp - Parses the `col` with the `format` to a timestamp. The function always
// returns null on an invalid input with/without ANSI SQL mode enabled. The result data type is
// consistent with the value of configuration `spark.sql.timestampType`.
//
// TryToTimestamp is the Golang equivalent of try_to_timestamp: (col: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryToTimestamp(col column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_to_timestamp", col, format))
}

// Xpath - Returns a string array of values within the nodes of xml that match the XPath expression.
//
// Xpath is the Golang equivalent of xpath: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Xpath(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath", xml, path))
}

// XpathBoolean - Returns true if the XPath expression evaluates to true, or if a matching node is found.
//
// XpathBoolean is the Golang equivalent of xpath_boolean: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathBoolean(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_boolean", xml, path))
}

// XpathDouble - Returns a double value, the value zero if no match is found,
// or NaN if a match is found but the value is non-numeric.
//
// XpathDouble is the Golang equivalent of xpath_double: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathDouble(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_double", xml, path))
}

// XpathNumber - Returns a double value, the value zero if no match is found,
// or NaN if a match is found but the value is non-numeric.
//
// XpathNumber is the Golang equivalent of xpath_number: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathNumber(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_number", xml, path))
}

// XpathFloat - Returns a float value, the value zero if no match is found,
// or NaN if a match is found but the value is non-numeric.
//
// XpathFloat is the Golang equivalent of xpath_float: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathFloat(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_float", xml, path))
}

// XpathInt - Returns an integer value, or the value zero if no match is found,
// or a match is found but the value is non-numeric.
//
// XpathInt is the Golang equivalent of xpath_int: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathInt(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_int", xml, path))
}

// XpathLong - Returns a long integer value, or the value zero if no match is found,
// or a match is found but the value is non-numeric.
//
// XpathLong is the Golang equivalent of xpath_long: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathLong(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_long", xml, path))
}

// XpathShort - Returns a short integer value, or the value zero if no match is found,
// or a match is found but the value is non-numeric.
//
// XpathShort is the Golang equivalent of xpath_short: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathShort(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_short", xml, path))
}

// XpathString - Returns the text contents of the first xml node that matches the XPath expression.
//
// XpathString is the Golang equivalent of xpath_string: (xml: 'ColumnOrName', path: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func XpathString(xml column.Column, path column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xpath_string", xml, path))
}

// Trunc - Returns date truncated to the unit specified by the format.
//
// Trunc is the Golang equivalent of trunc: (date: 'ColumnOrName', format: str) -> pyspark.sql.connect.column.Column
func Trunc(date column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("trunc", date, lit_format))
}

// DateTrunc - Returns timestamp truncated to the unit specified by the format.
//
// DateTrunc is the Golang equivalent of date_trunc: (format: str, timestamp: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func DateTrunc(format string, timestamp column.Column) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("date_trunc", lit_format, timestamp))
}

// NextDay - Returns the first date which is later than the value of the date column
// based on second `week day` argument.
//
// NextDay is the Golang equivalent of next_day: (date: 'ColumnOrName', dayOfWeek: str) -> pyspark.sql.connect.column.Column
func NextDay(date column.Column, dayOfWeek string) column.Column {
	lit_dayOfWeek := StringLit(dayOfWeek)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("next_day", date, lit_dayOfWeek))
}

// LastDay - Returns the last day of the month which the given date belongs to.
//
// LastDay is the Golang equivalent of last_day: (date: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func LastDay(date column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("last_day", date))
}

// FromUnixtime - Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
// representing the timestamp of that moment in the current system time zone in the given
// format.
//
// FromUnixtime is the Golang equivalent of from_unixtime: (timestamp: 'ColumnOrName', format: str = 'yyyy-MM-dd HH:mm:ss') -> pyspark.sql.connect.column.Column
func FromUnixtime(timestamp column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("from_unixtime", timestamp, lit_format))
}

// UnixTimestamp - Convert time string with given pattern ('yyyy-MM-dd HH:mm:ss', by default)
// to Unix time stamp (in seconds), using the default timezone and the default
// locale, returns null if failed.
//
// if `timestamp` is None, then it returns current timestamp.
//
// UnixTimestamp is the Golang equivalent of unix_timestamp: (timestamp: Optional[ForwardRef('ColumnOrName')] = None, format: str = 'yyyy-MM-dd HH:mm:ss') -> pyspark.sql.connect.column.Column
func UnixTimestamp(timestamp column.Column, format string) column.Column {
	lit_format := StringLit(format)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("unix_timestamp", timestamp, lit_format))
}

// FromUtcTimestamp - This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
// takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and
// renders that timestamp as a timestamp in the given time zone.
//
// However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
// timezone-agnostic. So in Spark this function just shift the timestamp value from UTC timezone to
// the given timezone.
//
// This function may return confusing result if the input is a string with timezone, e.g.
// '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
// according to the timezone in the string, and finally display the result by converting the
// timestamp to string according to the session local timezone.
//
// FromUtcTimestamp is the Golang equivalent of from_utc_timestamp: (timestamp: 'ColumnOrName', tz: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func FromUtcTimestamp(timestamp column.Column, tz column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("from_utc_timestamp", timestamp, tz))
}

// ToUtcTimestamp - This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function
// takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given
// timezone, and renders that timestamp as a timestamp in UTC.
//
// However, timestamp in Spark represents number of microseconds from the Unix epoch, which is not
// timezone-agnostic. So in Spark this function just shift the timestamp value from the given
// timezone to UTC timezone.
//
// This function may return confusing result if the input is a string with timezone, e.g.
// '2018-03-13T06:18:23+00:00'. The reason is that, Spark firstly cast the string to timestamp
// according to the timezone in the string, and finally display the result by converting the
// timestamp to string according to the session local timezone.
//
// ToUtcTimestamp is the Golang equivalent of to_utc_timestamp: (timestamp: 'ColumnOrName', tz: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func ToUtcTimestamp(timestamp column.Column, tz column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_utc_timestamp", timestamp, tz))
}

// TimestampSeconds - Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z)
// to a timestamp.
//
// TimestampSeconds is the Golang equivalent of timestamp_seconds: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampSeconds(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_seconds", col))
}

// TimestampMillis - Creates timestamp from the number of milliseconds since UTC epoch.
//
// TimestampMillis is the Golang equivalent of timestamp_millis: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampMillis(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_millis", col))
}

// TimestampMicros - Creates timestamp from the number of microseconds since UTC epoch.
//
// TimestampMicros is the Golang equivalent of timestamp_micros: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func TimestampMicros(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("timestamp_micros", col))
}

// Window - Bucketize rows into one or more time windows given a timestamp specifying column. Window
// starts are inclusive but the window ends are exclusive, e.g. 12:05 will be in the window
// [12:05,12:10) but not in [12:00,12:05). Windows can support microsecond precision. Windows in
// the order of months are not supported.
//
// The time column must be of :class:`pyspark.sql.types.TimestampType`.
//
// Durations are provided as strings, e.g. '1 second', '1 day 12 hours', '2 minutes'. Valid
// interval strings are 'week', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond'.
// If the “slideDuration“ is not provided, the windows will be tumbling windows.
//
// The startTime is the offset with respect to 1970-01-01 00:00:00 UTC with which to start
// window intervals. For example, in order to have hourly tumbling windows that start 15 minutes
// past the hour, e.g. 12:15-13:15, 13:15-14:15... provide `startTime` as `15 minutes`.
//
// The output column will be a struct called 'window' by default with the nested columns 'start'
// and 'end', where 'start' and 'end' will be of :class:`pyspark.sql.types.TimestampType`.
//
// Window is the Golang equivalent of window: (timeColumn: 'ColumnOrName', windowDuration: str, slideDuration: Optional[str] = None, startTime: Optional[str] = None) -> pyspark.sql.connect.column.Column
func Window(timeColumn column.Column, windowDuration string, slideDuration string, startTime string) column.Column {
	lit_windowDuration := StringLit(windowDuration)
	lit_slideDuration := StringLit(slideDuration)
	lit_startTime := StringLit(startTime)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("window", timeColumn,
		lit_windowDuration, lit_slideDuration, lit_startTime))
}

// WindowTime - Computes the event time from a window column. The column window values are produced
// by window aggregating operators and are of type `STRUCT<start: TIMESTAMP, end: TIMESTAMP>`
// where start is inclusive and end is exclusive. The event time of records produced by window
// aggregating operators can be computed as “window_time(window)“ and are
// “window.end - lit(1).alias("microsecond")“ (as microsecond is the minimal supported event
// time precision). The window column must be one produced by a window aggregating operator.
//
// WindowTime is the Golang equivalent of window_time: (windowColumn: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func WindowTime(windowColumn column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("window_time", windowColumn))
}

// TODO: session_window: (timeColumn: 'ColumnOrName', gapDuration: Union[pyspark.sql.connect.column.Column, str]) -> pyspark.sql.connect.column.Column

// ToUnixTimestamp - Returns the UNIX timestamp of the given time.
//
// ToUnixTimestamp is the Golang equivalent of to_unix_timestamp: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToUnixTimestamp(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_unix_timestamp", timestamp, format))
}

// ToTimestampLtz - Parses the `timestamp` with the `format` to a timestamp without time zone.
// Returns null with invalid input.
//
// ToTimestampLtz is the Golang equivalent of to_timestamp_ltz: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToTimestampLtz(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp_ltz", timestamp, format))
}

// ToTimestampNtz - Parses the `timestamp` with the `format` to a timestamp without time zone.
// Returns null with invalid input.
//
// ToTimestampNtz is the Golang equivalent of to_timestamp_ntz: (timestamp: 'ColumnOrName', format: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func ToTimestampNtz(timestamp column.Column, format column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("to_timestamp_ntz", timestamp, format))
}

// TODO: bucket: (numBuckets: Union[pyspark.sql.connect.column.Column, int], col: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// Years - Partition transform function: A transform for timestamps and dates
// to partition data into years.
//
// Years is the Golang equivalent of years: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Years(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("years", col))
}

// Months - Partition transform function: A transform for timestamps and dates
// to partition data into months.
//
// Months is the Golang equivalent of months: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Months(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("months", col))
}

// Days - Partition transform function: A transform for timestamps and dates
// to partition data into days.
//
// Days is the Golang equivalent of days: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Days(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("days", col))
}

// Hours - Partition transform function: A transform for timestamps
// to partition data into hours.
//
// Hours is the Golang equivalent of hours: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hours(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hours", col))
}

// TODO: convert_timezone: (sourceTz: Optional[pyspark.sql.connect.column.Column], targetTz: pyspark.sql.connect.column.Column, sourceTs: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// MakeDtInterval - Make DayTimeIntervalType duration from days, hours, mins and secs.
//
// MakeDtInterval is the Golang equivalent of make_dt_interval: (days: Optional[ForwardRef('ColumnOrName')] = None, hours: Optional[ForwardRef('ColumnOrName')] = None, mins: Optional[ForwardRef('ColumnOrName')] = None, secs: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeDtInterval(days column.Column, hours column.Column, mins column.Column, secs column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_dt_interval", days, hours, mins, secs))
}

// MakeInterval - Make interval from years, months, weeks, days, hours, mins and secs.
//
// MakeInterval is the Golang equivalent of make_interval: (years: Optional[ForwardRef('ColumnOrName')] = None, months: Optional[ForwardRef('ColumnOrName')] = None, weeks: Optional[ForwardRef('ColumnOrName')] = None, days: Optional[ForwardRef('ColumnOrName')] = None, hours: Optional[ForwardRef('ColumnOrName')] = None, mins: Optional[ForwardRef('ColumnOrName')] = None, secs: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeInterval(years column.Column, months column.Column, weeks column.Column,
	days column.Column, hours column.Column, mins column.Column, secs column.Column,
) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_interval", years,
		months, weeks, days, hours, mins, secs))
}

// MakeTimestamp - Create timestamp from years, months, days, hours, mins, secs and timezone fields.
// The result data type is consistent with the value of configuration `spark.sql.timestampType`.
// If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL
// on invalid inputs. Otherwise, it will throw an error instead.
//
// MakeTimestamp is the Golang equivalent of make_timestamp: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName', timezone: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeTimestamp(years column.Column, months column.Column, days column.Column,
	hours column.Column, mins column.Column, secs column.Column, timezone column.Column,
) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp", years,
		months, days, hours, mins, secs, timezone))
}

// MakeTimestampLtz - Create the current timestamp with local time zone from years, months, days, hours, mins,
// secs and timezone fields. If the configuration `spark.sql.ansi.enabled` is false,
// the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
//
// MakeTimestampLtz is the Golang equivalent of make_timestamp_ltz: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName', timezone: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeTimestampLtz(years column.Column, months column.Column, days column.Column,
	hours column.Column, mins column.Column, secs column.Column, timezone column.Column,
) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp_ltz",
		years, months, days, hours, mins, secs, timezone))
}

// MakeTimestampNtz - Create local date-time from years, months, days, hours, mins, secs fields.
// If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL
// on invalid inputs. Otherwise, it will throw an error instead.
//
// MakeTimestampNtz is the Golang equivalent of make_timestamp_ntz: (years: 'ColumnOrName', months: 'ColumnOrName', days: 'ColumnOrName', hours: 'ColumnOrName', mins: 'ColumnOrName', secs: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func MakeTimestampNtz(years column.Column, months column.Column, days column.Column,
	hours column.Column, mins column.Column, secs column.Column,
) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_timestamp_ntz",
		years, months, days, hours, mins, secs))
}

// MakeYmInterval - Make year-month interval from years, months.
//
// MakeYmInterval is the Golang equivalent of make_ym_interval: (years: Optional[ForwardRef('ColumnOrName')] = None, months: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func MakeYmInterval(years column.Column, months column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("make_ym_interval", years, months))
}

// CurrentCatalog - Returns the current catalog.
//
// CurrentCatalog is the Golang equivalent of current_catalog: () -> pyspark.sql.connect.column.Column
func CurrentCatalog() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_catalog"))
}

// CurrentDatabase - Returns the current database.
//
// CurrentDatabase is the Golang equivalent of current_database: () -> pyspark.sql.connect.column.Column
func CurrentDatabase() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_database"))
}

// CurrentSchema - Returns the current database.
//
// CurrentSchema is the Golang equivalent of current_schema: () -> pyspark.sql.connect.column.Column
func CurrentSchema() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_schema"))
}

// CurrentUser - Returns the current database.
//
// CurrentUser is the Golang equivalent of current_user: () -> pyspark.sql.connect.column.Column
func CurrentUser() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("current_user"))
}

// User - Returns the current database.
//
// User is the Golang equivalent of user: () -> pyspark.sql.connect.column.Column
func User() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("user"))
}

// TODO: assert_true: (col: 'ColumnOrName', errMsg: Union[pyspark.sql.connect.column.Column, str, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: raise_error: (errMsg: Union[pyspark.sql.connect.column.Column, str]) -> pyspark.sql.connect.column.Column

// Crc32 - Calculates the cyclic redundancy check value  (CRC32) of a binary column and
// returns the value as a bigint.
//
// Crc32 is the Golang equivalent of crc32: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Crc32(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("crc32", col))
}

// Hash - Calculates the hash code of given columns, and returns the result as an int column.
//
// Hash is the Golang equivalent of hash: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Hash(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hash", vals...))
}

// Xxhash64 - Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm,
// and returns the result as a long column. The hash computation uses an initial seed of 42.
//
// Xxhash64 is the Golang equivalent of xxhash64: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Xxhash64(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("xxhash64", vals...))
}

// Md5 - Calculates the MD5 digest and returns the value as a 32 character hex string.
//
// Md5 is the Golang equivalent of md5: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Md5(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("md5", col))
}

// Sha1 - Returns the hex string result of SHA-1.
//
// Sha1 is the Golang equivalent of sha1: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sha1(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha1", col))
}

// Sha2 - Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384,
// and SHA-512). The numBits indicates the desired bit length of the result, which must have a
// value of 224, 256, 384, 512, or 0 (which is equivalent to 256).
//
// Sha2 is the Golang equivalent of sha2: (col: 'ColumnOrName', numBits: int) -> pyspark.sql.connect.column.Column
func Sha2(col column.Column, numBits int64) column.Column {
	lit_numBits := Int64Lit(numBits)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha2", col, lit_numBits))
}

// TODO: hll_sketch_agg: (col: 'ColumnOrName', lgConfigK: Union[int, pyspark.sql.connect.column.Column, NoneType] = None) -> pyspark.sql.connect.column.Column

// TODO: hll_union_agg: (col: 'ColumnOrName', allowDifferentLgConfigK: Optional[bool] = None) -> pyspark.sql.connect.column.Column

// HllSketchEstimate - Returns the estimated number of unique values given the binary representation
// of a Datasketches HllSketch.
//
// HllSketchEstimate is the Golang equivalent of hll_sketch_estimate: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func HllSketchEstimate(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("hll_sketch_estimate", col))
}

// TODO: hll_union: (col1: 'ColumnOrName', col2: 'ColumnOrName', allowDifferentLgConfigK: Optional[bool] = None) -> pyspark.sql.connect.column.Column

// Ifnull - Returns `col2` if `col1` is null, or `col1` otherwise.
//
// Ifnull is the Golang equivalent of ifnull: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Ifnull(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("ifnull", col1, col2))
}

// Isnotnull - Returns true if `col` is not null, or false otherwise.
//
// Isnotnull is the Golang equivalent of isnotnull: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Isnotnull(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("isnotnull", col))
}

// EqualNull - Returns same result as the EQUAL(=) operator for non-null operands,
// but returns true if both are null, false if one of the them is null.
//
// EqualNull is the Golang equivalent of equal_null: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func EqualNull(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("equal_null", col1, col2))
}

// Nullif - Returns null if `col1` equals to `col2`, or `col1` otherwise.
//
// Nullif is the Golang equivalent of nullif: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nullif(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nullif", col1, col2))
}

// Nvl - Returns `col2` if `col1` is null, or `col1` otherwise.
//
// Nvl is the Golang equivalent of nvl: (col1: 'ColumnOrName', col2: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nvl(col1 column.Column, col2 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nvl", col1, col2))
}

// Nvl2 - Returns `col2` if `col1` is not null, or `col3` otherwise.
//
// Nvl2 is the Golang equivalent of nvl2: (col1: 'ColumnOrName', col2: 'ColumnOrName', col3: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Nvl2(col1 column.Column, col2 column.Column, col3 column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("nvl2", col1, col2, col3))
}

// AesEncrypt - Returns an encrypted value of `input` using AES in given `mode` with the specified `padding`.
// Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`,
// `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). Optional initialization
// vectors (IVs) are only supported for CBC and GCM modes. These must be 16 bytes for CBC and 12
// bytes for GCM. If not provided, a random vector will be generated and prepended to the
// output. Optional additional authenticated data (AAD) is only supported for GCM. If provided
// for encryption, the identical AAD value must be provided for decryption. The default mode is
// GCM.
//
// AesEncrypt is the Golang equivalent of aes_encrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, iv: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func AesEncrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, iv column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("aes_encrypt", input, key, mode, padding, iv, aad))
}

// AesDecrypt - Returns a decrypted value of `input` using AES in `mode` with `padding`. Key lengths of 16,
// 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB',
// 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). Optional additional authenticated data (AAD) is
// only supported for GCM. If provided for encryption, the identical AAD value must be provided
// for decryption. The default mode is GCM.
//
// AesDecrypt is the Golang equivalent of aes_decrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func AesDecrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("aes_decrypt", input, key, mode, padding, aad))
}

// TryAesDecrypt - This is a special version of `aes_decrypt` that performs the same operation,
// but returns a NULL value instead of raising an error if the decryption cannot be performed.
// Returns a decrypted value of `input` using AES in `mode` with `padding`. Key lengths of 16,
// 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB',
// 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS'). Optional additional authenticated data (AAD) is
// only supported for GCM. If provided for encryption, the identical AAD value must be provided
// for decryption. The default mode is GCM.
//
// TryAesDecrypt is the Golang equivalent of try_aes_decrypt: (input: 'ColumnOrName', key: 'ColumnOrName', mode: Optional[ForwardRef('ColumnOrName')] = None, padding: Optional[ForwardRef('ColumnOrName')] = None, aad: Optional[ForwardRef('ColumnOrName')] = None) -> pyspark.sql.connect.column.Column
func TryAesDecrypt(input column.Column, key column.Column, mode column.Column, padding column.Column, aad column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("try_aes_decrypt", input, key, mode, padding, aad))
}

// Sha - Returns a sha1 hash value as a hex string of the `col`.
//
// Sha is the Golang equivalent of sha: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Sha(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("sha", col))
}

// InputFileBlockLength - Returns the length of the block being read, or -1 if not available.
//
// InputFileBlockLength is the Golang equivalent of input_file_block_length: () -> pyspark.sql.connect.column.Column
func InputFileBlockLength() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_block_length"))
}

// InputFileBlockStart - Returns the start offset of the block being read, or -1 if not available.
//
// InputFileBlockStart is the Golang equivalent of input_file_block_start: () -> pyspark.sql.connect.column.Column
func InputFileBlockStart() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("input_file_block_start"))
}

// Reflect - Calls a method with reflection.
//
// Reflect is the Golang equivalent of reflect: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Reflect(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("reflect", vals...))
}

// JavaMethod - Calls a method with reflection.
//
// JavaMethod is the Golang equivalent of java_method: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func JavaMethod(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("java_method", vals...))
}

// Version - Returns the Spark version. The string contains 2 fields, the first being a release version
// and the second being a git revision.
//
// Version is the Golang equivalent of version: () -> pyspark.sql.connect.column.Column
func Version() column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("version"))
}

// Typeof - Return DDL-formatted type string for the data type of the input.
//
// Typeof is the Golang equivalent of typeof: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Typeof(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("typeof", col))
}

// Stack - Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default
// unless specified otherwise.
//
// Stack is the Golang equivalent of stack: (*cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func Stack(cols ...column.Column) column.Column {
	vals := make([]column.Column, 0)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("stack", vals...))
}

// BitmapBitPosition - Returns the bit position for the given input column.
//
// BitmapBitPosition is the Golang equivalent of bitmap_bit_position: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapBitPosition(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_bit_position", col))
}

// BitmapBucketNumber - Returns the bucket number for the given input column.
//
// BitmapBucketNumber is the Golang equivalent of bitmap_bucket_number: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapBucketNumber(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_bucket_number", col))
}

// BitmapConstructAgg - Returns a bitmap with the positions of the bits set from all the values from the input column.
// The input column will most likely be bitmap_bit_position().
//
// BitmapConstructAgg is the Golang equivalent of bitmap_construct_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapConstructAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_construct_agg", col))
}

// BitmapCount - Returns the number of set bits in the input bitmap.
//
// BitmapCount is the Golang equivalent of bitmap_count: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapCount(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_count", col))
}

// BitmapOrAgg - Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column.
// The input column should be bitmaps created from bitmap_construct_agg().
//
// BitmapOrAgg is the Golang equivalent of bitmap_or_agg: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func BitmapOrAgg(col column.Column) column.Column {
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("bitmap_or_agg", col))
}

// Ignore UDF: call_udf: (udfName: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// Ignore UDT: unwrap_udt: (col: 'ColumnOrName') -> pyspark.sql.connect.column.Column

// TODO: udf: (f: Union[Callable[..., Any], ForwardRef('DataTypeOrString'), NoneType] = None, returnType: 'DataTypeOrString' = StringType(), useArrow: Optional[bool] = None) -> Union[ForwardRef('UserDefinedFunctionLike'), Callable[[Callable[..., Any]], ForwardRef('UserDefinedFunctionLike')]]

// Ignore UDT: udtf: (cls: Optional[Type] = None, *, returnType: Union[pyspark.sql.types.StructType, str], useArrow: Optional[bool] = None) -> Union[ForwardRef('UserDefinedTableFunction'), Callable[[Type], ForwardRef('UserDefinedTableFunction')]]

// CallFunction - Call a SQL function.
//
// CallFunction is the Golang equivalent of call_function: (funcName: str, *cols: 'ColumnOrName') -> pyspark.sql.connect.column.Column
func CallFunction(funcName string, cols ...column.Column) column.Column {
	lit_funcName := StringLit(funcName)
	vals := make([]column.Column, 0)
	vals = append(vals, lit_funcName)
	vals = append(vals, cols...)
	return column.NewColumn(column.NewUnresolvedFunctionWithColumns("call_function", vals...))
}
