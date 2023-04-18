package com.skrbics.hadoop.Taxi;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TaxiJob {
	public static void main(String[] args) throws Exception {
		JobClient my_client = new JobClient();
		JobConf job_conf1 = new JobConf(TaxiJob.class);

		job_conf1.setOutputKeyClass(IntWritable.class);
		job_conf1.setOutputValueClass(IntWritable.class);

		job_conf1.setMapperClass(MapperTaxiOne.class);
		job_conf1.setReducerClass(ReducerTaxiOne.class);

		job_conf1.setInputFormat(TextInputFormat.class);
		job_conf1.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
		
		my_client.setConf(job_conf1);
		try { 
			JobClient.runJob(job_conf1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		JobConf job_conf2 = new JobConf(TaxiJob.class);
		job_conf2.setOutputKeyClass(IntWritable.class);
		job_conf2.setOutputValueClass(IntWritable.class);

		job_conf2.setMapperClass(MapperTaxiTwo.class);
		job_conf2.setReducerClass(ReducerTaxiTwo.class);

		job_conf2.setInputFormat(TextInputFormat.class);
		job_conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_conf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));
		
		my_client.setConf(job_conf2);
		try { 
			JobClient.runJob(job_conf2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
