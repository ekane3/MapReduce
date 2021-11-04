package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class DistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    /*
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void mapr(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
    */

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){
            context.write(new IntWritable(Integer.parseInt(value.toString().split(";")[1])),new IntWritable(1));
        }
        line++;
    }

}
