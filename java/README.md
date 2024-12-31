# Sample Spark-Submit Wrapper

This directory provides a simple wrapper library that can be used to submit a Spark Connect Go application to a Spark Cluster. 

## Wrapper Library

The wrapper library expects to variable input values:

1. The path to the binary file that contains the Spark Connect Go application. This path is specified via the Spark conf property `spark.golang.binary`.
2. The actual binary has to be submitted as part of the application using the `--files` parameter to the `spark-submit` script.

Building the libary can be done using:

```bash
sbt package
```

## Run Script

The `run.sh` script is a simple script that can be used to submit a Spark Connect Go application to a Spark Cluster. The script can be called as follows:

```bash
export SPARK_HOME=/path/to/spark
./run.sh ../cmd/spark-connect-example-spark-session/spark-connect-example-spark-session
```

When this is called from the current directory and with the Spark Connect Golang client build, it will submit the example application to the Spark Cluster.

The `run.sh` script can be modified according to your needs.