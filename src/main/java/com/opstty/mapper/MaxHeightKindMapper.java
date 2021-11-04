package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightKindMapper extends Mapper<Object, Text, Text, FloatWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){

            try{
                //Lets get the heights of the trees
                Float hauteur = Float.parseFloat(value.toString().split(";")[6]);
                context.write(new Text(value.toString().split(";")[3]),new FloatWritable(hauteur));
            }catch(NumberFormatException ex) {
                // System.out.println(""+ ex);
            }

        }
        line++;
    }

}
