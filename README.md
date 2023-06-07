## CS236 Project

A project that implements MapReduce on hotel booking data to find the most
profitable month.

#### members
Christopher Moussa (Student ID 862327381)

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

This is helpful, but we want to sort these key-value pairs by revenue and not
by key. We also might be interested in enriching the data in formats _besides_
just by month-year. So, the repository contains another script written in Python
that is responsible for sorting the key-value pairs, both in the initial format
required by the project in addition to a couple of other interesting orders.

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

#### detailed contribution by partner

##### christopher moussa

* set up GitHub repository for project
* wrote MapReduce Java code
* wrote data output sorting Python code
* created project README and project description
* created high-level diagram for both MapReduce job and project workflow
(linked in repository and in README)

#### references

[1]: [DoubleWritable Java Class](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/DoubleWritable.html)

[2]: [Timsort Algorithm](https://en.wikipedia.org/wiki/Timsort)
