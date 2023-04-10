package com.skrbics.hadoop.AvgPrice;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class APReducer extends MapReduceBase
implements Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    public void reduce(Text key, Iterator<DoubleWritable> values, 
    		OutputCollector<Text,DoubleWritable> output,
    		Reporter reporter) throws IOException 
    {
        double sum = 0;
        int num_of_prices = 0;
        DoubleWritable value;
        while(values.hasNext())
        {
        	value = values.next();
            sum += value.get();
            num_of_prices++;
        }

        output.collect(key, new DoubleWritable((double) sum / num_of_prices));
    }
}