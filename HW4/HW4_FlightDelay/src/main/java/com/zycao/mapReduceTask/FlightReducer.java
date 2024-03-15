package com.zycao.mapReduceTask;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlightReducer extends Reducer<Text, Text, IntWritable, Text> {

    private HashMap<String, List<String>> f1Flights = new HashMap<>();
    private List<String> f2Flights = new ArrayList<>();

    /**
     * Gather flights depending on first flight or second flight.
     * Put all F1 flights into a hashmap, key being data/otherAirport.
     * Put all F2 flights into a list for iteration in cleanup phase.
     * In the meantime, prune out unnecessary fields (year/month)
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Extracting the type, date, and intermediate airport from the key
        String[] keyParts = key.toString().split("/");
        String flightType = keyParts[0];
        String dateAndAirport = keyParts[1] + "/" + keyParts[2]; // This forms the new key (Date + Intermediate Airport)

        if (flightType.equals("F1")) {
            // If it's an F1 flight, accumulate in the HashMap
            f1Flights.computeIfAbsent(dateAndAirport, k -> new ArrayList<>());
            values.forEach(value -> {
                String[] valueParts = value.toString().split("/");
                String timeAndDelay = valueParts[2] + "/" + valueParts[3];
                f1Flights.get(dateAndAirport).add(timeAndDelay);
            });
        } else if (flightType.equals("F2")) {
            // If it's an F2 flight, add it to the ArrayList
            values.forEach(value -> {
                String[] valueParts = value.toString().split("/");
                String timeAndDelay = valueParts[2] + "/" + valueParts[3];
                f2Flights.add(dateAndAirport + " " + timeAndDelay);
            });
        }
    }

    /**
     * Iterate through all second flight, find potential first flight based on date/otherAirport
     * Check the Arrival time and Departure time of two flights
     * If they form a valid pair, accumulate total delay and pair counts
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        float totalDelay = 0;
        int validPairCount = 0;

        for (String f2 : f2Flights) {
            String[] f2Parts = f2.split(" ");
            String f2Key = f2Parts[0]; // "date/airport" as key
            String f2Detail[] = f2Parts[1].split("/");
            String f2DepTime = f2Detail[0];
            String f2Delay = f2Detail[1];

            // Check if there are corresponding F1 flights for the same date and airport
            if (f1Flights.containsKey(f2Key)) {
                for (String f1 : f1Flights.get(f2Key)) {
                    String f1Detail[] = f1.split("/");
                    String f1ArrTime = f1Detail[0];
                    String f1Delay = f1Detail[1];
                    // A valid pair found, accumulate results
                    if (f1ArrTime.compareTo(f2DepTime) < 0) {
                        totalDelay += (Float.parseFloat(f1Delay) + Float.parseFloat(f2Delay));
                        validPairCount++;
                    }
                }
            }
        }
        context.write(new IntWritable(validPairCount), new Text(Float.toString(totalDelay)));
    }

}
