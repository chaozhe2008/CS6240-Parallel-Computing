package com.zycao.secondarySort;

import com.zycao.model.CompositeKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


/**
 * A customized partitioiner to distribute tasks (carriers) evenly
 */
public class FlightPartitioner extends Partitioner<CompositeKey, FloatWritable> {
    private static final HashMap<String, Integer> airlineToPartitionMap = new HashMap<>();
    static {
        String[] airlines = {"AA", "B6", "UA", "OH", "DL", "EV", "NW", "WN", "YV", "9E", "AQ", "CO", "FL", "F9", "HA", "XE", "AS", "MQ", "OO", "US"};
        for (int i = 0; i < airlines.length; i++) {
            int index = i ;
            airlineToPartitionMap.put(airlines[i], index);
        }
    }

    @Override
    public int getPartition(CompositeKey key, FloatWritable value, int numPartitions) {
        int idx = airlineToPartitionMap.getOrDefault(key.getCarrier(), 0);
        return idx % numPartitions;
    }
}
