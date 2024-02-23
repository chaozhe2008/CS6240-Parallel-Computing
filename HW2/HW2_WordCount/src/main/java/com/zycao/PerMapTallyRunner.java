package com.zycao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PerMapTallyRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(PerMapTallyRunner.class);
        //--------Set PerMap Mapper class-------------//
        job.setMapperClass(PerMapTokenizerMapper.class);

        //--------Disable Combiner--------------
        //job.setCombinerClass(IntSumReducer.class);

        //--------Set task number and Partitioner-------
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(6);
        //----------------------------------------------

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
