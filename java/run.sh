#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

SCALA_VERSION=2.13
SPARK_VERSION=4.0.0

if [ -z "$SPARK_HOME" ]; then
  echo "SPARK_HOME must be set to run this script."
  exit 1
fi

BINARY_PATH=$1

if [ -z "$BINARY_PATH" ]; then
  echo "Usage: $0 <path-to-binary>"
  exit 1
fi

# Check if the binary exists.
if [ ! -f "$BINARY_PATH" ]; then
  echo "Binary not found: $BINARY_PATH, make sure the path is valid."
  exit 1
fi

# Get the absolute path of the binary.
BINARY_PATH=$(realpath $BINARY_PATH)
BINARY_NAME=$(basename $BINARY_PATH)

# Call the spark-submit script.
$SPARK_HOME/bin/spark-submit \
  --files $BINARY_PATH \
  --conf spark.golang.binary=$BINARY_NAME \
  --class org.apache.spark.golang.Runner \
  --packages org.apache.spark:spark-connect_$SCALA_VERSION:$SPARK_VERSION \
  target/scala-$SCALA_VERSION/sparkconnectgorunner_$SCALA_VERSION-0.1.0-SNAPSHOT.jar