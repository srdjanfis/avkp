package com.skrbics.hadoop.MaxTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MaxTempMapper extends MapReduceBase
                    implements Mapper<Object, Text, Text, IntWritable> 
{
    public void map(Object key, 
    				Text value, 
    				OutputCollector<Text, IntWritable> output,     
    				Reporter reporter) throws IOException 
    {
        try
        {
            if(value.toString().contains("Temperature")) // remove header
                return;
            else 
            {
                String record = value.toString();
                String[] parts = record.split(", ");

                output.collect(new Text(parts[0]), 
                		       new IntWritable(Integer.parseInt(parts[1])));
            }
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        }
    }
}