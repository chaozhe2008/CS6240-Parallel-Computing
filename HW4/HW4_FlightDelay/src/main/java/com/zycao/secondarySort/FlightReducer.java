package com.zycao.secondarySort;

import com.zycao.model.CompositeKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightReducer extends Reducer<CompositeKey, FloatWritable, Text, Text> {

    /**
     * in each reduce call, handle the accumulation and calculation for one unique carrier
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(CompositeKey key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float totalDelay = 0;
        int totalFlights = 0;
        int currentMonth = key.getMonth();
        Text outputKey = new Text(key.getCarrier());
        StringBuilder monthlyAverages = new StringBuilder();

        // GroupingComparator guarantees all values belong to same carrier
        // Since the month is in sorted order, we can accumulate the result in its original order
        for (FloatWritable val : values) {
            if (key.getMonth() == currentMonth) {
                totalDelay += val.get();
                totalFlights++;
            } else { // encounter a new month, log the previous month pair
                if (totalFlights > 0){
                    monthlyAverages.append(buildOutputPair(currentMonth, totalFlights, totalDelay)).append(", ");
                }
                totalDelay = val.get();
                totalFlights = 1;
                currentMonth = key.getMonth();
            }
        }

        // last month
        if (totalFlights > 0) {
            monthlyAverages.append(buildOutputPair(currentMonth, totalFlights, totalDelay));
        }
        context.write(outputKey, new Text(monthlyAverages.toString()));
    }

    private String buildOutputPair(int month, int total, float delay){
        int averageDelay = Math.round(delay / total);
        return "(" + month +  ", " + averageDelay + ")";
    }
}


