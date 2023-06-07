## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

#### members
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

#### about the code

![mapreduce-diagram](https://github.com/cmoussa1/cs236-project/blob/main/mapreduce%20diagram.png)

The Java source code runs a MapReduce job on a couple of large `.csv` file that
contain revenue data for a hotel that spans a couple of years. We do this by
creating an instance of the `Job` class; this represents the Hadoop job that
is executed by our code.

```java
Job job = Job.getInstance(conf, "hotel-revenue");
```

We set our mapper, combiner, and reducer classes for the jobs. The use of the
`Combiner` class is useful in optimizing the Hadoop job to be run; it performs
a local reduce operation _before_ sending the data to the `Reducer` class. It
uses the intermediate key-value pairs that are output by the mapper portion of
the job, which is then sent to the reducer. The benefit of this is it decreases
the volume of data sent. It's important to note that it does the same things as
the `Reducer` class, but operates within the output of each mapper.

We create our key in the `map()` function according to the arrival month and
year because we want to associate revenue to a specific month/year. The mapper
will output this key along with the average price per room, and this data will
be sent to the reducer.

Within the `Reducer` class, we pass the key-value pairs we created in `map()`,
where the average price per room values are iterated over and applied to a
`total_revenue` variable. This becomes the final value applied to each key
containing a month/year.

The key-value pairs are written to an output file where they are sorted by key:

```
1-2016	145597.68999999994
1-2018	75796.26000000004
10-2015	391084.6299999965
.
.
.
9-2018	364047.0699999994
```

This is helpful, but we want to sort these key-value pairs by revenue and not
by key. So, the repository contains another script written in Python that is
responsible for sorting the key-value pairs.

The script parses the output file line-by-line and creates a key-value pair for
each month/year-to-revenue pair:

```python
tmp_dict = dict(item.split('=') for item in line.split(','))
```

We want to insert each key-value pair into a dictionary that holds all of them
(note that we clean up the value for each pair slightly by rounding to two
decimal places to represent an actual US dollar/cents value):

```python
revenue_dict[i] = round(float(tmp_dict[i]), 2)
```

We then sort the dictionary by value:

```python
sorted_revenue = sorted(revenue_dict.items(), key=lambda x:x[1])
```

We pass in our unsorted dictionary as a list of tuples and use Python's built-in
`sorted()` function to return a sorted list based on the key we pass in, which
in this case, tells it to sort the tuples based on the _value_ instead of the
_key_. This creates a sorted list which is stored in `sorted_revenue`.

The time complexity of calling `sorted()` on this list of tuples is O(n log n)
by using the Timsort sorting algorithm[1].

#### references

[1]: [Timsort algorithm](https://en.wikipedia.org/wiki/Timsort)
