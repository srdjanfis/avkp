package com.skrbics;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HTPMReducer extends MapReduceBase
implements Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable>
{
    public void reduce(IntWritable key, Iterator<IntWritable> values, 
    		OutputCollector<IntWritable,DoubleWritable> output,
    		Reporter reporter) throws IOException 
    {
    	 
    	int sum = 0;
        int value;
        while(values.hasNext())
        {
        	value = values.next().get();
        	sum += value;
        }
        System.out.println("RED: "+key+", "+((double)sum)/12);
        output.collect(key, new DoubleWritable(((double)sum)/12));
    }
}
