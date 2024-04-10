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

1. [Download Spark distribution](https://spark.apache.org/downloads.html) (3.4.0+), unzip the package.

2. Start the Spark Connect server with the following command:

```
sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```

Step 5: Run the example Go application.

```
go run cmd/spark-connect-example-spark-session/main.go
```

## How to write Spark Connect Go Application in your own project

See [Quick Start Guide](quick-start.md)

## High Level Design

Following [diagram](https://textik.com/#ac299c8f32c4c342) shows main code in current prototype:

```
    +-------------------+                                                                              
    |                   |                                                                              
    |   dataFrameImpl   |                                                                              
    |                   |                                                                              
    +-------------------+                                                                              
              |                                                                                        
              |                                                                                        
              +                                                                                        
    +-------------------+                                                                              
    |                   |                                                                              
    | sparkSessionImpl  |                                                                              
    |                   |                                                                              
    +-------------------+                                                                              
              |                                                                                        
              |                                                                                        
              +                                                                                        
+---------------------------+               +----------------+                                         
|                           |               |                |                                         
| SparkConnectServiceClient |--------------+|  Spark Driver  |                                         
|                           |               |                |                                         
+---------------------------+               +----------------+
```

`SparkConnectServiceClient` is GRPC client which talks to Spark Driver. `sparkSessionImpl` generates `dataFrameImpl`
instances. `dataFrameImpl` uses the GRPC client in `sparkSessionImpl` to communicate with Spark Driver.

We will mimic the logic in Spark Connect Scala implementation, and adopt Go common practices, e.g. returning `error` object for
error handling.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.