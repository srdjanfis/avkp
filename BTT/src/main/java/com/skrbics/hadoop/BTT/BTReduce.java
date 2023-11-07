package com.skrbics.hadoop.BTT;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/* input:  <bank, num_of_transfers@transfer_amount>
 * output: <bank, num_of_transfers,sum_of_transfers>
 */
public class BTReduce extends MapReduceBase
implements Reducer<Text, Text, Text, Text>  
{
    public void reduce(Text key, Iterator<Text> values,
    		OutputCollector<Text,Text> output, 
			   Reporter reporter) 
					   throws IOException
    {
        int num_of_transfers = 0;
        int temp_num_of_transfers = 0;
        int sum_of_transfers = 0;
        while(values.hasNext())
        {
        	Text value = values.next();
            String[] splitted_value = value.toString().split("@");
            temp_num_of_transfers = Integer.parseInt(splitted_value[0]);
            num_of_transfers += temp_num_of_transfers;
            sum_of_transfers += Integer.parseInt(splitted_value[1]) * temp_num_of_transfers;
        }
        output.collect(key, new Text(String.valueOf(num_of_transfers) + 
        							 ' ' + 
        							 String.valueOf(sum_of_transfers)));
    }
}
