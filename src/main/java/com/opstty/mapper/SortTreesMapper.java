package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortTreesMapper extends Mapper<Object, Text, FloatWritable, Text> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){

            try{
                String[] line_tokens = value.toString().split(";");
                Float height = Float.parseFloat(line_tokens[6]);
                context.write(new FloatWritable(height),
                        new Text(line_tokens[11] + " - " + line_tokens[2] + " " + line_tokens[3] + " (" + line_tokens[4] + ")"));
            }catch(NumberFormatException ex) {
                // System.out.println(""+ ex);
            }

        }
        line++;
    }

}
