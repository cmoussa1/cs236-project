## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

#### Members
Christopher Moussa

Ramesh Subramaniam

#### instructions for compilation and run

compile the Java file:

```console
$ javac -classpath $(hadoop classpath) MapReduceHadoop.java
$ jar cf hotel-revenue.jar MapReduceHadoop*.class
```

place the input files in a directory with HDFS:

```console
$ hdfs dfs -mkdir /input
$ hdfs dfs -put path/to/customer-reservations.csv /path/to/hotel-booking.csv /input
```

submit the Hadoop job:

```console
$ hadoop jar hotel-revenue.jar MapReduceHadoop /input/customer-reservations.csv /input/hotel-booking.csv /output
```

place output of the Hadoop job on local disk:

```console
$ hdfs dfs -get /output path/to/local_disk
```

once you have the output file of your Hadoop job, you can sort the output by revenue (highest month/year revenue
at the top) by running the `sort-output.py` Python script:

```console
$ python3 sort-output.py path/to/output-file
```
