# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This is a basic script to generate the builtin functions based on the
# currently available PySpark installation.
# Simply call the script as follows:
#
# python gen.py > spark/client/functions/generated.go

import pyspark.sql.connect.functions as F
import inspect
import typing
import types

def normalize(input: str) -> str:
    vals = [x[0].upper() + x[1:] for x in input.split("_")]
    return "".join(vals)


print("""
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
""")


for fun in F.__dict__:
    if fun.startswith("_"):
        continue

    if not callable(F.__dict__[fun]):
        continue

    if "pyspark.sql.connect.functions" not in F.__dict__[fun].__module__:
        continue

    if fun == "expr" or fun == "col" or fun == "column" or fun == "lit":
        continue

    # Ignore the aliases of the old distinct.
    if "Distinct" in fun:
        continue

    sig = inspect.signature(F.__dict__[fun])

    # Ignore all functions that take callables as parameters
    has_callable = False
    for p in sig.parameters:
        param = sig.parameters[p]
        if "Callable" in str(param.annotation):
            has_callable = True
            break

    if has_callable:
        print(f"// TODO: {fun}: {sig}")
        print()
        continue

    if "udf" in fun.lower():
        print(f"// Ignore UDF: {fun}: {sig}")
        print()
        continue

    if "udt" in fun.lower():
        print(f"// Ignore UDT: {fun}: {sig}")
        print()
        continue

    # Convert parameters into Golang
    res_params = []
    conversions = []
    args = []
    valid = True
    for p in sig.parameters:
        param = sig.parameters[p]
        if param.annotation == inspect.Parameter.empty:
            res_params.append(f"{p} interface{{}}")
            args.append(p)
        elif param.kind == inspect.Parameter.VAR_POSITIONAL and param.annotation == "ColumnOrName":
            res_params.append(f"{p} ...column.Column")
            conversions.append("vals := make([]column.Column, 0)")
            for x in args:
                conversions.append(f"vals = append(vals, {x})")
            conversions.append(f"vals = append(vals, {p}...)")
            args = ["vals..."]
        elif type(param.annotation) == str and str(param.annotation) == "ColumnOrName" and param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.VAR_KEYWORD:
            res_params.append(f"{p} column.Column")
            args.append(p)
        elif len(typing.get_args(param.annotation)) > 1 and typing.ForwardRef("ColumnOrName") in typing.get_args(param.annotation):
            # Find the parameter with ColumnOrName
            tmp = [x for x in typing.get_args(param.annotation) if typing.ForwardRef("ColumnOrName") == x]
            assert len(tmp) == 1
            res_params.append(f"{p} column.Column")
            args.append(p)
        elif param.annotation == str or typing.get_args(param.annotation) == (str, types.NoneType):
            res_params.append(f"{p} string")
            conversions.append(f"lit_{p} := Lit({p})")
            args.append(f"lit_{p}")
        elif param.annotation == int or typing.get_args(param.annotation) == (int, types.NoneType):
            res_params.append(f"{p} int64")
            conversions.append(f"lit_{p} := Lit({p})")
            args.append(f"lit_{p}")
        elif param.annotation == float or typing.get_args(param.annotation) == (float, types.NoneType):
            res_params.append(f"{p} float64")
            conversions.append(f"lit_{p} := Lit({p})")
            args.append(f"lit_{p}")
        else:
            valid = False
            break

    if not valid:
        print(f"// TODO: {fun}: {sig}")
        print()
    else:
        name = normalize(fun)
        print(f"// {name} is the Golang equivalent of {fun}: {sig}")
        print(f"func {name}({', '.join(res_params)}) column.Column {{")
        for c in conversions:
            print(f"    {c}")
        print(f"    return column.NewColumn(column.NewUnresolvedFunctionWithColumns(\"{fun}\", {', '.join(args)}))")
        print(f"}}")
        print()