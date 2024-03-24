package com.zycao.HCompute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class HComputeDriver {
    public static void main(String[] args) throws Exception {
        // For EMR run
        Configuration conf = HBaseConfiguration.create();
        String hbaseSite="/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());

        Job job = Job.getInstance(conf, "HCompute");
        job.setJarByClass(HComputeDriver.class);
        Scan scan = new Scan();

        //Set scan range
        scan.setStartRow(Bytes.toBytes("2008_"));
        scan.setStopRow(Bytes.toBytes("2009_"));
        scan.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("arrDelay"));

        // Filter out cancelled flights
        SingleColumnValueFilter filterCancelled = new SingleColumnValueFilter(
                Bytes.toBytes("family1"),
                Bytes.toBytes("cancelled"),
                CompareOperator.NOT_EQUAL,
                Bytes.toBytes("0.00")
        );
        FilterList filterList = new FilterList();
        filterList.addFilter(filterCancelled);
        scan.setFilter(filterList);

        TableMapReduceUtil.initTableMapperJob(
                args[0],
                scan,
                HComputeMapper.class,
                Text.class,
                FloatWritable.class,
                job);
        job.setPartitionerClass(CarrierPartitioner.class);
        job.setReducerClass(HComputeReducer.class);
        job.setNumReduceTasks(10);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
