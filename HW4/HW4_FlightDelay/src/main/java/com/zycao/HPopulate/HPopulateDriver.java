package com.zycao.HPopulate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import java.io.File;

public class HPopulateDriver {
    public static void main(String[] args) throws Exception {
        // For EMR run
        Configuration conf = HBaseConfiguration.create();
        String hbaseSite="/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());

        Job job = Job.getInstance(conf, "HPopulate");
        job.setJarByClass(HPopulateDriver.class);
        job.setMapperClass(HPopulateMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        // set output class as TableOutputFormat, no need for reducer
        job.setNumReduceTasks(0);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(args[1]);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("family1")).build());

                // Split table into 3 regions, ensuring all data in year 2008 in the same region
                byte[][] splitKeys = {Bytes.toBytes("2008_"), Bytes.toBytes("2009_")};
                admin.createTable(tableDescriptorBuilder.build(), splitKeys);
                System.out.println("Created table " + tableName);
            }
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
