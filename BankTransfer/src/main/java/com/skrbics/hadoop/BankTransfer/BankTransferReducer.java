package com.skrbics.hadoop.BankTransfer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BankTransferReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterator<Text> values, 
    		OutputCollector<Text,Text> output,
    		Reporter reporter) throws IOException 
    {
    	 
    	int sum = 0;
        int num_of_transfers = 0;
        Text value;
        String[] data;
        while(values.hasNext())
        {
        	value = values.next();
        	data = value.toString().split("@");
            sum += (Integer.parseInt(data[1]));
            num_of_transfers++;
        }
        output.collect(key, new Text(""+num_of_transfers+" "+sum));
    }
}
