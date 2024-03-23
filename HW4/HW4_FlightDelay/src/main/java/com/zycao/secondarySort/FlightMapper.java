package com.zycao.secondarySort;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.zycao.model.CompositeKey;
import com.zycao.model.FlightRecord;
import com.zycao.util.DataParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, CompositeKey, FloatWritable> {

    private CSVParser csvParser;

    /**
     * Set up a csvParser for mapper function
     *
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
     *
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
            CompositeKey compositeKey = new CompositeKey(record.getCarrier(), Integer.parseInt(record.getMonth()));
            context.write(compositeKey, new FloatWritable(Float.parseFloat(record.getArrDelay())));
        }
    }

}
