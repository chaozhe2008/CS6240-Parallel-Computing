package com.zycao.mapReduceTask;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AggregateReducer extends Reducer<Text, Text, Text, Text> {
    private long totalPairCount = 0;
    private float totalDelaySum = 0;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if(parts.length == 2) {
                long pairCount = Long.parseLong(parts[0].trim());
                float delaySum = Float.parseFloat(parts[1].trim());

                totalPairCount += pairCount;
                totalDelaySum += delaySum;
            }
        }

        if (totalPairCount > 0) {
            float averageDelay = totalDelaySum / totalPairCount;
            context.write(new Text(String.valueOf(totalPairCount)), new Text(String.valueOf(averageDelay)));
        } else {
            context.write(new Text("Error"), new Text("No Pair Found"));
        }
    }
}
