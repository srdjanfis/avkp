package com.skrbics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TDMapper extends MapReduceBase 
implements Mapper<Object, Text, Text, DoubleWritable> 
{
	public void map(Object key, Text value, 
			OutputCollector<Text, DoubleWritable> output, 
			Reporter reporter)
					throws IOException {

		try 
		{ 

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line,",");
			String day = tokenizer.nextToken();
			DoubleWritable temp = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));

			output.collect(new Text(day),temp);

		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}