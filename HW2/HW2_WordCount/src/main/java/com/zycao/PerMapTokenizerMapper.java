package com.zycao;

import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class PerMapTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Initialize a HashMap for each map function call
        HashMap<String, Integer> wordCountMap = new HashMap<>();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            // Trim off the trailing punctuation marks (except single quote)
            String token = itr.nextToken().replaceAll("[\\p{Punct}&&[^\\']]+$", "");

            if (token.matches("^[mnoqpMNOQP].*")) {
                wordCountMap.put(token, wordCountMap.getOrDefault(token, 0) + 1);
            }
        }

        // Emit the key-value pairs at the end of map function call
        for (HashMap.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            word.set(entry.getKey());
            context.write(word, new IntWritable(entry.getValue()));
        }
    }
}