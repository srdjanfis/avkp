package com.skrbics.hadoop.BankTransfer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class BankTransferMapper  extends MapReduceBase 
implements Mapper<Object, Text, Text, Text> 
{
	public void map(Object key, Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter)
					throws IOException {

		try 
		{ 
			if(value.toString().contains("Amount")) // remove header
				return;
			else 
			{
				String[] data = value.toString().split(" ");
				String bank = data[0];
				output.collect(new Text(bank), new Text("1@"+data[2]));
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}


