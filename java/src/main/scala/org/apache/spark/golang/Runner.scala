/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.golang

import scala.sys.process._

import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * This is the main entry point for the Spark Connect Go runner.
 *
 * To run any Go code on your Spark cluster using spark-submit, you can use
 * this very simple wrapper to do so. To
 */
object Runner extends Logging {
  def main(args: Array[String]): Unit = {
    // Instantiate a new Spark Context.
    val ctx = SparkContext.getOrCreate()
    // Start the SparkConnect service which will listen for incoming requests.
    SparkConnectService.start(ctx)

    // Create a new Spark Session to fetch the port configuration that the service
    // listens on.
    val spark = SparkSession.builder().getOrCreate()
    val port = spark.conf.get("spark.connect.grpc.binding.port").toInt

    // Fetch the binary of the program to be executed.
    val bin = spark.conf.get("spark.golang.binary")

    // Fetch the local path of the binary.
    val path = SparkFiles.get(bin)
    path.! // Run the binary
    logWarning("Stopping Spark Connect service")
    SparkConnectService.stop()
    ctx.stop()
  }
}