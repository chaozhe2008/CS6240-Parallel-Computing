package com.zycao.HCompute;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * A customized partitioiner to distribute tasks (carriers) evenly
 */
public class CarrierPartitioner extends Partitioner<Text, FloatWritable> {
    private static final HashMap<String, Integer> airlineToPartitionMap = new HashMap<>();

    static {
        String[] airlines = {"AA", "B6", "UA", "OH", "DL", "EV", "NW", "WN", "YV", "9E", "AQ", "CO", "FL", "F9", "HA", "XE", "AS", "MQ", "OO", "US"};
        for (int i = 0; i < airlines.length; i++) {
            airlineToPartitionMap.put(airlines[i], i);
        }
    }

    @Override
    public int getPartition(Text key, FloatWritable value, int numPartitions) {
        String carrier = key.toString().split("_")[0];
        int partition = airlineToPartitionMap.getOrDefault(carrier, 0);
        return partition % numPartitions;
    }
}
