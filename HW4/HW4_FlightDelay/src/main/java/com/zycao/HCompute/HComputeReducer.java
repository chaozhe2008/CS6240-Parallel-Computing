package com.zycao.HCompute;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HComputeReducer extends Reducer<Text, FloatWritable, Text, Text> {
    private Map<String, float[][]> carrierMonthData = new HashMap<>();

    /**
     * parse and store delay data into a hashmap
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) {
        String[] parts = key.toString().split("_");
        String carrier = parts[0];
        int month = Integer.parseInt(parts[1]) - 1; // Convert month to 0-based index
        carrierMonthData.putIfAbsent(carrier, new float[12][2]);
        for (FloatWritable val : values) {
            float delay = val.get();
            carrierMonthData.get(carrier)[month][0] += delay; // Accumulate delay
            carrierMonthData.get(carrier)[month][1] += 1; // Increment count
        }
    }

    /**
     * After all data has been accumulated, calculate the average and write to output
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, float[][]> entry : carrierMonthData.entrySet()) {
            String carrier = entry.getKey();
            float[][] monthData = entry.getValue();
            StringBuilder resultBuilder = new StringBuilder();

            for (int month = 0; month < 12; month++) {
                if (monthData[month][1] > 0) { // Check if there are flights for the month
                    float averageDelay = monthData[month][0] / monthData[month][1];
                    resultBuilder.append(String.format("(%d, %d), ", month + 1, Math.round(averageDelay)));
                }
            }
            if (resultBuilder.length() > 0) {
                resultBuilder.setLength(resultBuilder.length() - 2); // Remove trailing comma and space
            }
            context.write(new Text(carrier), new Text(resultBuilder.toString()));
        }
    }
}
