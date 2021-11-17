package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeDistrictMapper extends Mapper<Object, Text, NullWritable, MapWritable> {

    public int curr_line = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (curr_line != 0) {
            try {
                Integer year = Integer.parseInt(value.toString().split(";")[5]);
                MapWritable map = new MapWritable();
                map.put(new IntWritable(Integer.parseInt(value.toString().split(";")[1])), new IntWritable(year));
                context.write(NullWritable.get(), map);
            } catch (NumberFormatException ex) {
                // If the year is not a integer, skip by catching the error from the parseFloat() method
            }
        } curr_line++;
    }

}
