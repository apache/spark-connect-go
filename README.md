# Apache Spark Connect Client for Golang

This project houses the **experimental** client for [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for
[Apache Spark](https://spark.apache.org/) written in [Golang](https://go.dev/).

## Current State of the Project

Currently, the Spark Connect client for Golang is highly experimental and should
not be used in any production setting. In addition, the PMC of the Apache Spark
project reserves the right to withdraw and abandon the development of this project
if it is not sustainable.

## Getting started

This section explains how to run Spark Connect Go locally.

Step 1: Install Golang: https://go.dev/doc/install.

Step 2: Ensure you have installed `buf CLI` installed, [more info here](https://buf.build/docs/installation/)

Step 3: Run the following commands to setup the Spark Connect client.

```
git clone https://github.com/apache/spark-connect-go.git
git submodule update --init --recursive

make gen && make test
```

Step 4: Setup the Spark Driver on localhost.

1. [Download Spark distribution](https://spark.apache.org/downloads.html) (4.0.0+), unzip the package.

2. Start the Spark Connect server with the following command (make sure to use a package version that matches your Spark distribution):

```
sbin/start-connect-server.sh
```

Step 5: Run the example Go application.

```
go run cmd/spark-connect-example-spark-session/main.go
```

## Runnning Spark Connect Go Application in a Spark Cluster

To run the Spark Connect Go application in a Spark Cluster, you need to build the Go application and submit it to the Spark Cluster. You can find a more detailed example runner and wrapper script in the `java` directory.

See the guide here: [Sample Spark-Submit Wrapper](java/README.md).

## How to write Spark Connect Go Application in your own project

See [Quick Start Guide](quick-start.md)

## High Level Design

The overall goal of the design is to find a good balance of principle of the least surprise for
develoeprs that are familiar with the APIs of Apache Spark and idiomatic Go usage. The high-level
structure of the packages follows roughly the PySpark giudance but with Go idioms.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
