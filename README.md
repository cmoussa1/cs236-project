## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

### Members
Christopher Moussa (Student ID 862327381)

Ramesh Subramaniam

---

### Instructions for Compilation and Run

in Windows, start Hadoop in Command Prompt as an administrator:

```console
C:\Hadoop\hadoop-3.3.0\sbin> start-all.cmd
```

remove any previous `/input` and `/output` directories (Hadoop will complain
about this if these directories already exist):

```console
$ hdfs -rm -r /input
$ hdfs -rm -r /output
```

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

once you have the output file of your Hadoop job, you can sort the output by
revenue (highest month/year revenue at the top) by running the `sort-output.py`
Python script and passing in the output file created by Hadoop:

```console
$ python3 sort-output.py path/to/output-file
```

---

### About the Code

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
year because we want to associate revenue to a specific month and year.

```java
String comp_key = arrival_mo + "-" + arrival_yr;

// output key
out_key.set(comp_key);
```

Parsing _most_ of the data we need is pretty straightforward. Because the data
is in `.csv` format, we can just fetch (and cast) the necessary data using an
array of `String`s:

```java
String[] csv_input_fields = value.toString().split(",");
```

Then, we can parse each field according to its index in the comma-delimited
line:

```java
avg_price_per_room = Double.parseDouble(csv_input_fields[8]);
arrival_yr = Integer.parseInt(csv_input_fields[4]);
arrival_mo = Integer.parseInt(csv_input_fields[5]);
```

Note that we still have to cast each field to its respective data type from
when it is initially read in the `.csv` file.

Because the data in `customer-reservations.csv` and `hotel-booking.csv` is
formatted differently, in our `map()` function, we need to convert some of
this data so we can correctly create our keys before sending them to be
reduced and have their revenue data aggregated. Specifically, the
"arrival_month" field in `hotel-booking.csv` is written in String format, not
in an integer format like `customer-reservations.csv`. So, the code contains a
`switch` statement to associate each "string" month with its integer
equivalent:

```java
switch(csv_input_fields[4]) {
  case "January":
    arrival_mo = 1;
    break;

  case "February":
    arrival_mo = 2;
    break;
  
  ...

  case "December":
    arrival_mo = 12;
    break;
}
```

The mapper will output this key along with the average price per room, and this
data will be sent to the reducer.

```java
out_key.set(comp_key);
out_value.set(avg_price_per_room);
```

Within the `Reducer` class, we pass the key-value pairs we created in `map()`,
where the average price per room values are iterated over and applied to a
`total_revenue` variable. This becomes the final value applied to each key
containing a month/year.

The `reduce()` function takes each month-year composite key, and for each key,
iterates over its `Double` values using a for-each loop[1]. They are then
aggregated to the `total_revenue` variable. So, at the end of each loop, the
key will have a sum of all revenue associated with the current key. In total,
there will be a for-each loop for every month-year key created in the `map()`
function.

```java
for (DoubleWritable value : values) {
  total_revenue += value.get();
}
```

The key-value pairs are written to an output file where they are sorted
_by key_:

```
1-2016	145597.68999999994
1-2018	75796.26000000004
10-2015	391084.6299999965
.
.
.
9-2018	364047.0699999994
```

### Hadoop Job Statistics

_note: these statistics are gathered from running the Hadoop job on my
(Christopher's) machine._

The Hadoop job on my (Christopher) machine has a total runtime of about 18
seconds.

The following console output blocks show snapshots of the statistics reported
by running the Hadoop job. Here is Hadoop reporting that the job ran
successfully:

```
2023-06-08 10:43:57,954 INFO mapreduce.Job:  map 0% reduce 0%
2023-06-08 10:44:03,046 INFO mapreduce.Job:  map 50% reduce 0%
2023-06-08 10:44:04,058 INFO mapreduce.Job:  map 100% reduce 0%
2023-06-08 10:44:08,105 INFO mapreduce.Job:  map 100% reduce 100%
2023-06-08 10:44:08,109 INFO mapreduce.Job: Job job_1686245305254_0002 completed successfully
```

File System Counters reports the following statistics about the HDFS
distribution:

```console
2023-06-08 10:44:08,160 INFO mapreduce.Job: Counters: 55
File System Counters
    FILE: Number of bytes read=630
    FILE: Number of bytes written=798077
    FILE: Number of read operations=0
    FILE: Number of large read operations=0
    FILE: Number of write operations=0
    HDFS: Number of bytes read=8164798
    HDFS: Number of bytes written=916
    HDFS: Number of read operations=11
    HDFS: Number of large read operations=0
    HDFS: Number of write operations=2
    HDFS: Number of bytes read erasure-coded=0
```

The Job Counters section breaks down the time spent by the numerous map and
reduce tasks:

```console
Job Counters
    Killed map tasks=1
    Launched map tasks=2
    Launched reduce tasks=1
    Rack-local map tasks=2
    Total time spent by all maps in occupied slots (ms)=3912
    Total time spent by all reduces in occupied slots (ms)=1969
    Total time spent by all map tasks (ms)=3912
    Total time spent by all reduce tasks (ms)=1969
    Total vcore-milliseconds taken by all map tasks=3912
    Total vcore-milliseconds taken by all reduce tasks=1969
    Total megabyte-milliseconds taken by all map tasks=4005888
    Total megabyte-milliseconds taken by all reduce tasks=2016256
```

And here are the counters for both the input and output files:

```console
File Input Format Counters
    Bytes Read=8164570
File Output Format Counters
    Bytes Written=916
```

### Sorting the Output of the Hadoop Job

The output file produced by the Hadoop job is helpful, but we want to sort
these key-value pairs by revenue and not by key. We also might be interested in
enriching the data in formats _besides_ just by month-year. So, the repository
contains another script written in Python that is responsible for sorting the
key-value pairs, both in the initial format required by the project in addition
to a couple of other interesting orders.

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
by using the Timsort sorting algorithm[2].

In terms of ranking the key-value pairs by their total revenue in decreasing
order, the Python script also sorts the data in a couple of other potentially
interesting formats:

* ranks total revenue by "season" (aggregates all "fall" months, "winter"
months, etc.)
* ranks total revenue by "year" (also in descending order)

The following code blocks show both the required order for the project as well
as some other interesting orders that are output by the Python script.
We start with the required order:

```console
8-2016: $723470.27
9-2016: $618959.16
10-2016: $589978.46
7-2016: $573715.3
6-2016: $566123.95
5-2016: $528075.29
9-2015: $484902.64
4-2016: $482651.9
8-2015: $411934.11
10-2015: $391084.63
3-2016: $381430.43
10-2018: $379361.24
9-2018: $364047.07
11-2016: $359605.18
6-2018: $358628.7
8-2018: $339280.11
12-2016: $333389.15
5-2018: $295680.14
7-2018: $295200.77
4-2018: $279068.72
2-2016: $272767.93
7-2015: $271588.06
11-2018: $225227.17
3-2018: $216661.76
12-2015: $216311.39
12-2018: $198725.08
10-2017: $176057.22
9-2017: $170399.72
1-2016: $145597.69
11-2015: $141757.79
2-2018: $137570.13
8-2017: $92366.68
1-2018: $75796.26
12-2017: $69999.47
11-2017: $46803.4
7-2017: $30815.24
```

We then re-calculate and output total revenue by season:

```console
max seasonal revenue was in fall: $3948183.68

seasonal revenue rankings (descending order):
   fall - $3948183.68
   summer - $3663123.19
   spring - $2183568.24
   winter - $1450157.1
```

We also re-calculate and output total revenue by year:

```console
max yearly revenue was in 2016: $5575764.71

yearly revenue rankings (descending order):
   2016 - $5575764.71
   2018 - $3165247.15
   2015 - $1917578.62
   2017 - $586441.73
```

The total runtime for this Python script is less than 1 second.

---

### Detailed Contribution by Partner

#### Christopher Moussa

* set up GitHub repository for project
* wrote MapReduce Java code
* wrote data output sorting Python code
* created project README and project description
* created high-level diagram for both MapReduce job and project workflow
(linked in repository and in README)

#### References

[1]: [DoubleWritable Java Class](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/DoubleWritable.html)

[2]: [Timsort Algorithm](https://en.wikipedia.org/wiki/Timsort)
