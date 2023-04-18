package com.skrbics.hadoop.Taxi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerTaxiOne extends MapReduceBase
implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
	public void reduce(IntWritable key, Iterator<IntWritable> values, 
			OutputCollector<IntWritable,IntWritable> output,
			Reporter reporter) throws IOException 
	{
		int diff = 0;
		IntWritable value;
		while(values.hasNext())
		{
			value = values.next();
			diff += value.get();
		}
		output.collect(key, new IntWritable(diff));
	}
}