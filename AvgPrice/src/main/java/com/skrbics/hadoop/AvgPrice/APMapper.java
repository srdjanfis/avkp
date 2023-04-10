package com.skrbics.hadoop.AvgPrice;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class APMapper extends MapReduceBase 
implements Mapper<Object, Text, Text, DoubleWritable> 
{
	public void map(Object key, Text value, 
			OutputCollector<Text, DoubleWritable> output, 
			Reporter reporter)
					throws IOException {

		try 
		{ 
			if(value.toString().contains("Address")) // remove header
				return;
			else 
			{
				String[] data = value.toString().split(",");
				String zipcode = data[3];
				DoubleWritable price = new DoubleWritable(Double.parseDouble(data[4]));

				output.collect(new Text(zipcode), price);
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}

