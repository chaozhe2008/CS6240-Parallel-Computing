package com.zycao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SiCombinerRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SiCombinerRunner.class);
        job.setMapperClass(TokenizerMapper.class);
        //--------Set combiner--------------
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        //--------Set task number and Partitioner-------
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(6);
        //----------------------------------------------

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.getInputFormatClass());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}