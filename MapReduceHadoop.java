/*
 *  CS236: MapReduce Job to find hotel revenue
 *
 *  author: christopher moussa
 */

import java.lang.reflect.Method;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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

        // set input paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2])); // output path

        // wait for job to complete; will output 0 on SUCCESS, 1 on FAILURE
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // mapper class
    public static class HotelBookingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text out_key = new Text();
        private DoubleWritable out_value = new DoubleWritable();

        private static final String FILE1_NAME = "customer-reservations.csv";
        private static final String FILE2_NAME = "hotel-booking.csv";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            // check which input file the current record is from;
            // this will determine how we parse the .csv file since
            // the headers are different for each file
            String inputFileTag;
            if (fileName.contains("customer-reservations")) {
                inputFileTag = FILE1_NAME;
            } else {
                inputFileTag = FILE2_NAME;
            }

            String[] csv_input_fields = value.toString().split(",");

            // skip header row of customer-reservations.csv
            if (csv_input_fields[0].equals("Booking_ID")) {
                return;
            }

            // skip header row of hotel-booking.csv
            if (csv_input_fields[0].equals("hotel")) {
                return;
            }

            double avg_price_per_room = 0.0;
            int arrival_yr = 0;
            int arrival_mo = 0;

            if (inputFileTag.contains("customer-reservations")) {
                // extract relevant fields for the map phase, consisting of:
                //
                // - avg_price_per_room
                // - arrival_year
                // - arrival_month
                // - booking_status

                // we need to check if the reservation was canceled or not;
                // if it is canceled, we should just set the price_per_room
                // to 0 because the customer never actually stayed there
                boolean canceled = csv_input_fields[9].equals("Canceled");
                if (canceled == true) {
                    avg_price_per_room = 0.0;
                } else {
                    avg_price_per_room = Double.parseDouble(csv_input_fields[8]);
                }
                arrival_yr = Integer.parseInt(csv_input_fields[4]);
                arrival_mo = Integer.parseInt(csv_input_fields[5]);
            } else {
                // we know that we are parsing hotel-booking, so we need to extract
                // the relevant fields for this specific .csv file, which consists
                // of:
                //
                // - avg_price_per_room
                // - arrival_year
                // - arrival_month
                // - booking_status

                // same check as for customer-reservations.csv; if the customer
                // canceled their reservation, we shouldn't add their revenue
                // to the total revenue for a given month/year
                boolean canceled = Integer.parseInt(csv_input_fields[1]) == 1;
                if (canceled == true) {
                    avg_price_per_room = 0.0;
                } else {
                    avg_price_per_room = Double.parseDouble(csv_input_fields[11]);
                }
                arrival_yr = Integer.parseInt(csv_input_fields[3]);
                // the arrival_month field in hotel-booking.csv is in String format,
                // so we need to map the month to an integer value
                switch(csv_input_fields[4]) {
                    case "January":
                        arrival_mo = 1;
                    break;

                    case "February":
                        arrival_mo = 2;
                    break;

                    case "March":
                        arrival_mo = 3;
                    break;

                    case "April":
                        arrival_mo = 4;
                    break;

                    case "May":
                        arrival_mo = 5;
                    break;

                    case "June":
                        arrival_mo = 6;
                    break;

                    case "July":
                        arrival_mo = 7;
                    break;

                    case "August":
                        arrival_mo = 8;
                    break;

                    case "September":
                        arrival_mo = 9;
                    break;

                    case "October":
                        arrival_mo = 10;
                    break;

                    case "November":
                        arrival_mo = 11;
                    break;

                    case "December":
                        arrival_mo = 12;
                    break;
                }
            }

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

            // aggregate revenues for the same month/year
            for (DoubleWritable value : values) {
                total_revenue += value.get();
            }

            // output the composite key and total revenue
            out_value.set(total_revenue);
            context.write(key, out_value);
        }
    }
}
