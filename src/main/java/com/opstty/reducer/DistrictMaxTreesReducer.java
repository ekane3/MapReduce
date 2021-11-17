package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DistrictMaxTreesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public ArrayList<Integer[]> sum_districts = new ArrayList<Integer[]>();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        sum_districts.add(new Integer[] {key.get(), sum });
    }

    public void cleanup(Context context) {
        int max = sum_districts.stream().map(arr -> arr[1]).max(Integer::compare).get();
        sum_districts.stream().filter(arr -> arr[1] == max)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(max));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
