package com.skrbics.hadoop.Taxi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperTaxiOne extends MapReduceBase 
implements Mapper<Object, Text, IntWritable, IntWritable> 
{
	public void map(Object key, Text value, 
			OutputCollector<IntWritable, IntWritable> output, 
			Reporter reporter)
					throws IOException {

		try 
		{ 
			IntWritable one = new IntWritable(1);
			IntWritable negativeOne = new IntWritable(-1);
			if(value.toString().contains("PickupLocationID")) // remove header
				return;
			else 
			{
				String[] data = value.toString().split(",");
				output.collect(new IntWritable(Integer.parseInt(data[0])), one);
				output.collect(new IntWritable(Integer.parseInt(data[1])), negativeOne);
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}