## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

### Members
Christopher Moussa (Student ID 862327381) cmous003@ucr.edu

Ramesh Subramaniam (Student ID 86322586) rsubr011@ucr.edu

---

### Instructions for Compilation and Run

in Windows, start Hadoop in Command Prompt as an administrator:

```console
C:\Hadoop\hadoop-3.3.0\sbin> start-all.cmd
```

remove any previous `/input` and `/output` directories (Hadoop will complain
about this if these directories already exist):

```console
$ hdfs dfs -rm -r /input
$ hdfs dfs -rm -r /output
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
$ python3 sort-output.py path/to/output-file > sorted_output.txt
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

The `map()` portion of MapReduceHadoop is responsible for processing the two
input files containing hotel reservation data. Each line of each input file is
passed in for processing [1].

We create our key in the `map()` function according to the arrival month and
year because we want to associate revenue to a specific month and year.

```java
String comp_key = arrival_mo + "-" + arrival_yr;

// output key
out_key.set(comp_key);
```

Parsing _most_ of the data we need is pretty straightforward. Because the data
is in `.csv` format, we can just fetch (and cast) the necessary data using an
array of `String`s on each line in the input files:

```java
String[] csv_input_fields = value.toString().split(",");
```

Then, we can parse each field according to its index in the comma-delimited
line. For the purposes of the project, we extract the following information
for every line in the input files:

- the month and year of the reservation

This will help us create our key.

- how many nights the reservation is for
- the average price per night per reservation

These two fields will help us calculate the cost of each reservation. In Java,
we parse each field and convert it to an appropriate data type for further
processing:

```java
avg_price_per_room = Double.parseDouble(csv_input_fields[8]);
arrival_yr = Integer.parseInt(csv_input_fields[4]);
arrival_mo = Integer.parseInt(csv_input_fields[5]);
stays_in_weekend_nights = Integer.parseInt(csv_input_fields[1]);
stays_in_week_nights = Integer.parseInt(csv_input_fields[2]);
```

Both `.csv` files contain data about the status of the reservation;
specifically, whether the reservation was fulfilled or not. If it was canceled,
the revenue from this reservation should **not** be added to the total
revenue for a given month/year key. So, the `map ()` function contains a check
for this field in both files, and if the reservation was canceled, it sets a
a revenue for a reservation room to 0. However, the data concerning the
reservation status in either input file is formatted differently, so we use
separate logic to handle either case. In `customer-reservations.csv`, the
field is a string: "Canceled" vs. "Not_Canceled":

```java
boolean canceled = csv_input_fields[9].equals("Canceled");
```

In `hotel-booking,csv`, the field is an integer: `1` for canceled and `0` for
not canceled:

```java
boolean canceled = Integer.parseInt(csv_input_fields[1]) == 1;
```

Mentioned above, the data in `customer-reservations.csv` and
`hotel-booking.csv` is formatted differently, so in our `map()` function, we
need to convert some of this data so we can correctly create our keys before
sending them to be reduced and have their revenue data aggregated. Specifically,
the "arrival_month" field in `hotel-booking.csv` is written in String format,
not in an integer format like `customer-reservations.csv`. So, the code
contains a `switch` statement to associate each "string" month with its integer
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

Finally, we need to calculate the total cost of each reservation. We accomplish
this by adding up the total number of nights stayed (`stays_in_weekend_nights` +
`stays_in_week_nights`) and multiplying it by the average price of the room
(`avg_price_per_room`):

```java
total_cost = total_nights * avg_price_per_room;
```

The mapper will output this key along with the the total cost, and this data
will be sent to the reducer.

```java
out_key.set(comp_key);
out_value.set(total_cost);
context.write(out_key, out_value);
```

Within the `Reducer` class, we pass the key-value pairs we created in `map()`,
where the total cost of all the reservations are iterated over and applied to a
`total_revenue` variable. This becomes the final value applied to each key
containing a month/year.

The `reduce()` function takes each month-year composite key, and for each key,
iterates over its `Double` values using a for-each loop [2]. They are then
aggregated to a `total_revenue` variable. So, at the end of each loop, the
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
1-2016	382496.53999999986
1-2018	293461.93999999994
10-2015	1152197.140000005
.
.
.
9-2018	777766.8400000002
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
to a couple of other interesting orders to attempt some extra credit. Note that
redirecting the output of the Python script to a file is important if you want
to keep the output of the script, i.e:

```console
$ python3 sort-output.py path/to/output-file > sorted_output.txt
                                             ^^^^^^^^^^^^^^^^^^^
```

The script parses the output file line-by-line and creates a key-value pair for
each month/year-to-revenue pair:

```python
tmp_dict = dict(item.split('=') for item in line.split(','))
```

We want to insert each key-value pair into a dictionary that holds all of them
(note that we clean up the value for each pair slightly by rounding to two
decimal places to represent an actual currency value):

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
by using the Timsort sorting algorithm [3].

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
8-2016: 2619095.32
7-2016: 2173409.5
9-2016: 1877089.8
6-2016: 1682397.44
8-2015: 1622785.1
5-2016: 1569233.94
9-2015: 1525256.12
10-2016: 1509066.1
4-2016: 1293336.9
3-2016: 1158918.14
10-2015: 1152197.14
7-2015: 1108981.14
11-2016: 1019546.48
12-2016: 975764.22
6-2018: 850484.1
10-2018: 829240.48
8-2018: 788780.94
12-2018: 781086.08
9-2018: 777766.84
5-2018: 753424.8
7-2018: 748720.72
4-2018: 722705.78
2-2016: 689667.68
3-2018: 651878.34
11-2018: 640184.84
12-2015: 640085.64
9-2017: 621614.08
10-2017: 615946.66
11-2015: 534079.04
2-2018: 409208.62
1-2016: 382496.54
12-2017: 317900.7
8-2017: 305291.94
1-2018: 293461.94
11-2017: 191827.48
7-2017: 38000.02
```

We then re-calculate and output total revenue by season (extra credit
opportunity #1):

```console
max seasonal revenue was in summer: 11937946.22

seasonal revenue rankings (descending order):
   summer - 11937946.22
   fall - 11293815.06
   spring - 6149497.9
   winter - 4489671.42
```

We also re-calculate and output total revenue by year (extra credit
opportunity #2):

```console
max yearly revenue was in 2016: 16950022.06

yearly revenue rankings (descending order):
   2016 - 16950022.06
   2018 - 8246943.48
   2015 - 6583384.18
   2017 - 2090580.88
```

We also re-calculate and output total revenue by month (extra credit
opportunity #3):

```console
max monthly revenue was in august: 5335953.3

monthly revenue rankings (descending order):
   august - 5335953.3
   september - 4801726.84
   october - 4106450.38
   july - 4069111.38
   december - 2714836.64
   june - 2532881.54
   november - 2385637.84
   may - 2322658.74
   april - 2016042.68
   march - 1810796.48
   february - 1098876.3
   january - 675958.48
```

The total runtime for this Python script is less than 1 second.

---

### Roadblocks, Confusions, and Obstacles, oh my!

This was the first time I had ever worked with Hadoop, and for that reason,
I faced a number of differently-sized roadblocks while coming up with the
implementation for this project.

Hadoop was kind of a pain to install on my Mac computer because of some Java
and JDK incompatibilities. Switching over to a Windows system and running
Hadoop as an administrator helped resolve this issue. However, specifying
the paths for the datanodes proved to be a much harder thing to do than I
originally thought:

```xml
<property> 
<name>dfs.datanode.data.dir</name> 
<value>/Hadoop/hadoop-3.3.0/data/datanode</value>
</property> 
```

It is **SO** important when installing Hadoop to point datanodes and namenodes
to the right locations. Not setting these correctly caused setting up HDFS
(which I _believe_ is needed before you can even run a Hadoop job) to
repeatedly fail.

There were other, smaller, issues with setting up Hadoop, and
I think one of the reasons for it is that Hadoop installation tutorials seem
to look different with every tutorial blog, so I had to use a few different
ones to set it up and run it correctly.

Another minor roadblock came up with testing the MapReduce code. I was never
quite able to figure out how to unit test my Java code, so I found myself
running Hadoop job after Hadoop job to figure out how the code was behaving.
Luckily, in my case, the Hadoop job runs (on average) in less than 20 seconds,
which would most likely have around the same runtime as running
a number of unit tests. So, I had to just bite the bullet and live with
re-compiling and re-submitting Hadoop jobs, one after the other, sometimes
getting bogus results and having to "spelunk" my code manually.

Trying to understand the statistics of the Hadoop job was also a little
confusing at the start. Hadoop outputs a lot of different statistics about the
job it ran and some of it does not appear straightforward at the start. Here's
one example:

```console
Total vcore-milliseconds taken by all map tasks=4266
Total vcore-milliseconds taken by all reduce tasks=2078
Total megabyte-milliseconds taken by all map tasks=4368384
Total megabyte-milliseconds taken by all reduce tasks=2127872
```

These didn't make a lot of sense to me at first, but a couple of Google
searches  helped clear these up. This data is essentially explaining how much
CPU time and memory was consumed by the mappers and reducers to perform their
respective operations during execution. Using the internet to help define the
various statistics about the Hadoop job helped me gain a better understanding
of How Hadoop breaks down large datasets and executes them over parallel tasks
in an efficient manner.

---

### Detailed Contribution by Partner

#### Christopher Moussa

* set up GitHub repository for project
* wrote MapReduce Java code
* wrote data output sorting Python code
* created project README and project description
* created high-level diagram for both MapReduce job and project workflow
(linked in repository and in README)

#### Ramesh Subramaniam

* helped design MapReduce Java class
* performed code review on `MapReduceHadoop.java` and `sort-output.py`
* prepared project presentation and demo

#### References

[1]: [MapReduce Description](https://www.tutorialspoint.com/hadoop/hadoop_mapreduce.htm)

[2]: [DoubleWritable Java Class](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/DoubleWritable.html)

[3]: [Timsort Algorithm](https://en.wikipedia.org/wiki/Timsort)
