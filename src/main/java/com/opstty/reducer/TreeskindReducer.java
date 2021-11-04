package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreeskindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val: values){
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
