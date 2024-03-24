package com.zycao.HPopulate;

import com.opencsv.CSVParser;
import com.zycao.model.FlightRecord;
import com.zycao.util.DataParser;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.UUID;

public class HPopulateMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private CSVParser csvParser = new CSVParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parsed = csvParser.parseLine(value.toString()); // parse each row into a flight object
        FlightRecord record = DataParser.convertToFlightRecord(parsed);

        if (record != null) {
            String uuid = UUID.randomUUID().toString();
            String rowKeyStr = record.getYear() + "_" + record.getCarrier() + "_" + record.getMonth() + "_" + uuid;
            // rowKey format: year + carrier + month + uuid (adding uuid to make row key unique)
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(rowKeyStr));

            Put put = new Put(rowKey.get());
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("year"), Bytes.toBytes(record.getYear()));
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("month"), Bytes.toBytes(record.getMonth()));
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("flightDate"), Bytes.toBytes(record.getFlightDate()));
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("carrier"), Bytes.toBytes(record.getCarrier()));
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("arrDelay"), Bytes.toBytes(record.getArrDelay()));
            put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("cancelled"), Bytes.toBytes(record.getCancelled()));
            context.write(rowKey, put);
        }
    }
}

