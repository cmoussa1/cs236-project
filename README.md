## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

#### Members
Christopher Moussa

Ramesh Subramaniam

#### instructions for compilation and run

compile java file:

```
$ javac -classpath $(hadoop classpath) MapReduceHadoop.java
$ jar cf hotel-revenue.jar MapReduceExample*.class
```

place input file to HDFS:

```
$ hdfs dfs -put path/to/input_file.csv /input
```

submit hadoop job:

```
hadoop jar hotel-revenue.jar MapReduceHadoop /input /output
```

place output on local disk:

```
hdfs dfs -get /output path/to/local_disk
```
