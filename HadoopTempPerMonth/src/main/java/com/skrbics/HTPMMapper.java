package com.skrbics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HTPMMapper  extends MapReduceBase 
implements Mapper<Object, Text, IntWritable, IntWritable> 
{
	public void map(Object key, Text value, 
			OutputCollector<IntWritable, IntWritable> output, 
			Reporter reporter)
					throws IOException {

		try 
		{ 
			if(value.toString().contains("Year")) // remove header
				return;
			else 
			{
				String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line,",");
		        IntWritable year = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
		        IntWritable temp;
		        while (tokenizer.hasMoreTokens()) {
		        	temp = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
		        	System.out.println("MAP: "+year+", "+temp);
		            output.collect(year,temp);
		        }
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}


