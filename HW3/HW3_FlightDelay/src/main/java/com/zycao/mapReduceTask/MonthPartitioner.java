package com.zycao.mapReduceTask;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class MonthPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String startDate = "2007-06-01";
        String date = key.toString().split("/")[1];
        return (convertToMonth(date) - convertToMonth(startDate)) % numReduceTasks;
    }

    private int convertToMonth(String date) {
        int yearNum = Integer.parseInt(date.substring(2, 4));
        int monthNum = Integer.parseInt(date.substring(5, 7));
        return 12 * yearNum + monthNum;
    }
}
