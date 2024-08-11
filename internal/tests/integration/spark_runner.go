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

package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
)

func StartSparkConnect() (int64, error) {
	sparkHome := os.Getenv("SPARK_HOME")
	if sparkHome == "" {
		return -1, sparkerrors.WithString(sparkerrors.TestSetupError, "SPARK_HOME not set")
	}

	fmt.Printf("Starting Spark Connect Server in: %v\n", os.Getenv("SPARK_HOME"))

	cmd := exec.Command("./sbin/start-connect-server.sh", "--wait", "--conf",
		"spark.log.structuredLogging.enabled=false")
	cmd.Dir = sparkHome

	err := cmd.Start()
	if err != nil {
		return -1, sparkerrors.WithType(sparkerrors.TestSetupError, err)
	}

	timeout := time.After(180 * time.Second)
	tick := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-timeout:
			return -1, sparkerrors.WithString(sparkerrors.TestSetupError,
				"timeout waiting for Spark Connect to start")
		case <-tick.C:
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				return -1, sparkerrors.WithString(sparkerrors.TestSetupError, "Spark Connect exited")
			}
			conn, err := net.Dial("tcp", "localhost:15002")
			if err == nil {
				conn.Close()
				return int64(cmd.Process.Pid), nil
			}
		}
	}
}

func StopSparkConnect(pid int64) error {
	return nil
}
