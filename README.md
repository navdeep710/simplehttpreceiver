A Simple Spark(streaming) java library ingesting HTTP based queue into spark streaming

Exposing streaming helper functions for pyspark demonstrating how to expose custom java streams to pyspark

To build the jar 

`mvn clean package`

Put the jar in driver classpath of spark with 

`pyspark --driver-class-path <path/to/above/jar>`