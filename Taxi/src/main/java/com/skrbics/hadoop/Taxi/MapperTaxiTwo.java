package com.skrbics.hadoop.Taxi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperTaxiTwo extends MapReduceBase 
implements Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	public void map(LongWritable key, Text value, 
			OutputCollector<IntWritable, IntWritable> output, 
			Reporter reporter)
					throws IOException {

		try 
		{
			StringTokenizer st = new StringTokenizer(value.toString());
			IntWritable keyCatch; 
			IntWritable valueCatch;
			while(st.hasMoreTokens()) {
				keyCatch = new IntWritable(Integer.parseInt(st.nextToken()));
				valueCatch = new IntWritable(Integer.parseInt(st.nextToken()));
				output.collect(valueCatch, keyCatch);
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}
