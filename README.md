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
$ jar cf hotel-revenue.jar MapReduceHadoop*.class
```

place input files to HDFS:

```
$ hdfs dfs -mkdir /input
$ hdfs dfs -put path/to/customer-reservations.csv /path/to/hotel-booking.csv /input
```

submit Hadoop job:

```
hadoop jar hotel-revenue.jar MapReduceHadoop /input/customer-reservations.csv /input/hotel-booking.csv /output
```

place output of Hadoop job on local disk:

```
hdfs dfs -get /output path/to/local_disk
```
