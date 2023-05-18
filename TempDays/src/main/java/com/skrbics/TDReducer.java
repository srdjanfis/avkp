package com.skrbics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TDReducer extends MapReduceBase
implements Reducer<Text, DoubleWritable, Text, Text>
{
	public void reduce(Text key, Iterator<DoubleWritable> values, 
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException 
	{
		ArrayList<Double> sorted=new ArrayList<Double>();
		while (values.hasNext()) {
			sorted.add(values.next().get());
		}
		Collections.sort(sorted, Collections.reverseOrder());
		
		String temp="";
		for (int i=0;i<5;i++){
			temp=temp+sorted.get(i)+"\t";
		}           	
		output.collect(key, new Text(temp));
	}
}
