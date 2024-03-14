package com.zycao.mapReduceTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "phase1");
        job1.setJarByClass(MapReduceDriver.class);

        // Set the input and output paths from the command line arguments
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job1.setMapperClass(FlightMapper.class);
        job1.setReducerClass(FlightReducer.class);
        job1.setPartitionerClass(MonthPartitioner.class);
        job1.setNumReduceTasks(12);

        // Set the output key and value types
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Set the input and output format classes
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "phase2");
        job2.setJarByClass(MapReduceDriver.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Set the Mapper and Reducer classes
        job2.setMapperClass(AggregateMapper.class);
        job2.setReducerClass(AggregateReducer.class);
        job2.setNumReduceTasks(1);

        // Set the output key and value types
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Set the input and output format classes
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
