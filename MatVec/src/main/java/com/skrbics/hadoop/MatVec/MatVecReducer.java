package com.skrbics.hadoop.MatVec;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatVecReducer  extends MapReduceBase
implements Reducer<IntWritable, Text, IntWritable, DoubleWritable>
{
    public void reduce(IntWritable key, Iterator<Text> values, 
    		OutputCollector<IntWritable,DoubleWritable> output,
    		Reporter reporter) throws IOException 
    {
    	 
    	int size = 3;
        Text value;
        double[] mat = new double[size];
        double[] vec = new double[size];
        double res = 0;
        while(values.hasNext())
        {
        	value = values.next();  	
        	String[] data = value.toString().split(",");
        	if(data[0].toString().equals("M")) {
        		mat[Integer.parseInt(data[1])]=Double.parseDouble(data[2]);
        	}else {
        		vec[Integer.parseInt(data[1])]=Double.parseDouble(data[2]);
        	}
        }
        for(int i=0;i<size;i++) {
        	res += mat[i]*vec[i];
        }
        output.collect(key, new DoubleWritable(res));
    }
}