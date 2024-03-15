package com.zycao.mapReduceTask;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class MonthPartitioner extends Partitioner<Text, Text> {

    /**
     * Partition key/value pairs based on flight month
     * @param key
     * @param value
     * @param numReduceTasks
     * @return
     */
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        int partition = Integer.valueOf(key.toString().split("/")[3]);
        return partition % numReduceTasks;
    }

}
