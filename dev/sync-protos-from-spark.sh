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
#

set -e

if [[ $# -ne 1 ]]; then
  echo "Illegal number of parameters."
  echo "Usage: ./dev/sync-protos-from-spark.sh [spark_home]"
  exit -1
fi

CONNECT_GO_HOME="$(cd "`dirname $0`"/..; pwd)"
pushd $CONNECT_GO_HOME
# Copy the files directly from the Spark repository.
cp -R $1/connector/connect/common/src/main/protobuf/spark $CONNECT_GO_HOME/proto
popd
