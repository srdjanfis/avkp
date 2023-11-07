package com.skrbics.hadoop.BTT;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/* input:  <byte_offset, line_of_dataset>
 * output: <bank, num_of_transfers@transfer_amount>
 */
public class BTMap extends MapReduceBase 
				implements Mapper<Object, Text, Text, Text> 
{
    public void map(Object key, 
			Text value, 
			OutputCollector <Text, Text> output, 
			Reporter reporter)  
    {
        try
        {
            if(value.toString().contains("Amount")) // remove header
                return;
            else 
            {
                String line = value.toString();
                String[] columns = line.split(" ");
                output.collect(new Text(columns[0]), new Text(columns[3] + "@" + columns[2]));
            }
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        }
    }
}
