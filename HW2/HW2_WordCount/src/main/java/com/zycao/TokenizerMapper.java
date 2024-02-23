package com.zycao;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            // Trim off the trailing punctuation marks (except single quote)
            String token = itr.nextToken().replaceAll("[\\p{Punct}&&[^\\']]+$", "");

            // Check if the word starts with "mnopq"
            if (token.matches("^[mnoqpMNOQP].*")) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}