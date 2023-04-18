package com.skrbics.hadoop.MatVec;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MatVecMapper extends MapReduceBase 
implements Mapper<Object, Text, IntWritable, Text> 
{
	public void map(Object key, Text value, 
			OutputCollector<IntWritable, Text> output, 
			Reporter reporter)
					throws IOException {

		try 
		{
			int size = 3;
			String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line," ");	        
	        String tempS = "";
	        IntWritable tempIW1;
	        IntWritable tempIW2;
	        DoubleWritable tempDW;
	        while (tokenizer.hasMoreTokens()) {
	            tempS = tokenizer.nextToken();          
	            if (tempS.equals("M")) { // Matrix entry
	                tempIW1 = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
	                tempIW2 = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
	                tempDW = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));
	                output.collect(tempIW1, new Text("M,"+tempIW2.get()+","+tempDW));
	            } else if (tempS.equals("b")) { // vector entry
	                	tempIW1 = new IntWritable(Integer.parseInt(tokenizer.nextToken()));
		                tempDW = new DoubleWritable(Double.parseDouble(tokenizer.nextToken()));
		            for (int i = 0; i < size; i++) {
		                output.collect(new IntWritable(i), new Text("b,"+tempIW1.get()+","+tempDW));   
	                }
	            }
	        }
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

}