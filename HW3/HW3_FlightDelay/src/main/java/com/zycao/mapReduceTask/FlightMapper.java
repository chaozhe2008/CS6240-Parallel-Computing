package com.zycao.mapReduceTask;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.zycao.model.FlightRecord;
import com.zycao.util.DataParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, Text, Text> {

    private CSVParser csvParser;

    /**
     * Set up a csvParser for mapper function
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        csvParser = new CSVParserBuilder().withSeparator(',').build();
    }

    /**
     * Perform filter on rows and classification based on start/end airport
     * Eliminate redundant fields
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header
        if (key.get() == 0 && value.toString().startsWith("\"Year\"")) {
            return;
        }

        String[] parsed = csvParser.parseLine(value.toString());
        FlightRecord record = DataParser.convertToFlightRecord(parsed);

        if (record != null && DataParser.isValid(record)) {
            // monthOrder is for partitioner
            Integer monthOrder = 12 * (Integer.parseInt(record.getYear()) - 2006) + Integer.parseInt(record.getMonth());
            String flightType = record.getOrigin().equals("ORD") ? "F1" : "F2";
            String otherAirport = flightType.equals("F1") ? record.getDest() : record.getOrigin();
            String compositeKey = flightType + "/" + record.getFlightDate() + "/" + otherAirport + "/" + monthOrder;
            String flightData = DataParser.convertToStringValue(flightType, record);
            context.write(new Text(compositeKey), new Text(flightData));
        }
    }

}
