package com.zycao;

import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class PerTaskTokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    // Initialize a HashMap for each task as counter
    private HashMap<String, Integer> wordCountMap = new HashMap<>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            // Trim off the trailing punctuation marks (except single quote)
            String token = itr.nextToken().replaceAll("[\\p{Punct}&&[^\\']]+$", "");

            // Check if the word starts with "mnopq"
            if (token.matches("^[mnoqpMNOQP].*")) {
                wordCountMap.put(token, wordCountMap.getOrDefault(token, 0) + 1);
            }
        }
    }

    // Emit the key-value pairs after task finishes
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (HashMap.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
        wordCountMap.clear();
    }
}
