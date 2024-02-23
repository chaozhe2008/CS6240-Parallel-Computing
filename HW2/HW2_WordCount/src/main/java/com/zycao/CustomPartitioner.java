package com.zycao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        }
        // distribute to partition by first character
        char firstChar = key.toString().toLowerCase().charAt(0);
        switch (firstChar) {
            case 'm':
                return 0 % numReduceTasks;
            case 'n':
                return 1 % numReduceTasks;
            case 'o':
                return 2 % numReduceTasks;
            case 'p':
                return 3 % numReduceTasks;
            case 'q':
                return 4 % numReduceTasks;
            default:
                return 5 % numReduceTasks;
        }
    }
}