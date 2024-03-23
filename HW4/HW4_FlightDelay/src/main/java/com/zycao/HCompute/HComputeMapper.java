package com.zycao.HCompute;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;

public class HComputeMapper extends TableMapper<Text, FloatWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String rowKey = Bytes.toString(key.get());
        String[] parts = rowKey.split("_");
        String carrierAndMonth = parts[1] + "_" + parts[2];
        byte[] arrDelayBytes = value.getValue(Bytes.toBytes("family1"), Bytes.toBytes("arrDelay"));
        String arrDelayStr = Bytes.toString(arrDelayBytes);
        context.write(new Text(carrierAndMonth), new FloatWritable(Float.parseFloat(arrDelayStr)));
    }
}
