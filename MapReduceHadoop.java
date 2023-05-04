/*
 *  CS236: MapReduce Job to find hotel revenue
 *
 *  author: christopher moussa
 */

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceHadoop {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "hotel-revenue");

        // set mapper, reducer, and combiner classes
        job.setJarByClass(MapReduceHadoop.class);
        job.setMapperClass(HotelBookingMapper.class);
        job.setCombinerClass(HotelBookingReducer.class);
        job.setReducerClass(HotelBookingReducer.class);

        // set output key/value classes for mapper and reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        // waits for job to complete; will output 0 on SUCCESS, 1 on FAILURE
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // mapper class
    public static class HotelBookingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text out_key = new Text();
        private DoubleWritable out_value = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] csv_input_fields = value.toString().split(",");

            // skip header row
            if (csv_input_fields[0].equals("Booking_ID")) {
                return;
            }

            // extract relevant fields for the map phase, consisting of:
            //
            // - avg_price_per_room
            // - arrival_year
            // - arrival_month
            double avg_price_per_room = Double.parseDouble(csv_input_fields[8]);
            int arrival_yr = Integer.parseInt(csv_input_fields[4]);
            int arrival_mo = Integer.parseInt(csv_input_fields[5]);

            // create month-year composite key to map revenue to
            String comp_key = arrival_mo + "-" + arrival_yr;

            // output key
            out_key.set(comp_key);
            out_value.set(avg_price_per_room);
            context.write(out_key, out_value);
        }
    }

    // reducer class
    public static class HotelBookingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable out_value = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total_revenue = 0.0;

            // Aggregate revenues for the same month/year
            for (DoubleWritable value : values) {
                total_revenue += value.get();
            }

            // Output the composite key and total revenue
            out_value.set(total_revenue);
            context.write(key, out_value);
        }
    }
}
