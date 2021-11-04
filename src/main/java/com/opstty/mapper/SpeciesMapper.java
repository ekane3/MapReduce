package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){
            context.write(new Text(value.toString().split(";")[3]),NullWritable.get());
        }
        line++;
    }

}
