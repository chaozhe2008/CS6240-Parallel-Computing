package com.zycao.secondarySort;

import com.zycao.model.CompositeKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AverageDelay");
        job.setJarByClass(MapReduceDriver.class);

        // Set the input and output paths from the command line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setPartitionerClass(FlightPartitioner.class);

        // Set Reducer Number
        job.setNumReduceTasks(10);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(FloatWritable.class);


        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true)? 0: 1);

    }
}
