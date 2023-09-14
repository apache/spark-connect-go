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
```
git clone https://github.com/apache/spark-connect-go.git
git submodule update --init --recursive

make gen && make test
```
> Ensure you have installed `buf CLI`; [more info](https://buf.build/docs/installation/)

## How to write Spark Connect Go Application in your own project

See [Quick Start Guide](quick-start.md)

## Spark Connect Go Application Example

A very simple example in Go looks like following:

```
func main() {
	remote := "localhost:15002"
	spark, _ := sql.SparkSession.Builder.Remote(remote).Build()
	defer spark.Stop()

	df, _ := spark.Sql("select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	df.Show(100, false)
}
```

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

## How to Run Spark Connect Go Application

1. Install Golang: https://go.dev/doc/install.

2. Download Spark distribution (3.4.0+), unzip the folder.

3. Start Spark Connect server by running command:

```
sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```

4. In this repo, run Go application:

```
go run cmd/spark-connect-example-spark-session/main.go
```

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.