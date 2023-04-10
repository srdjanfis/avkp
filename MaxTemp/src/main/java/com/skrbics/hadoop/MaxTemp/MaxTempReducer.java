package com.skrbics.hadoop.MaxTemp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MaxTempReducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, 
    				   Iterator<IntWritable> values, 
    				   OutputCollector<Text, IntWritable> output,
    				   Reporter reporter) throws IOException 
    {
        int max_value = -273; 
        IntWritable value;
        while(values.hasNext())
        {
        	value = values.next();
            if(value.get() > max_value)
                max_value = value.get();
        }
        output.collect(key, new IntWritable(max_value));
    }

}