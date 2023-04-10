package com.skrbics.hadoop.BankTransfer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class BankTransferJob {
	public static void main(String[] args) throws Exception {
		JobClient my_client = new JobClient();
		JobConf job_conf = new JobConf(BankTransferJob.class);

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(Text.class);

		job_conf.setMapperClass(BankTransferMapper.class);
		job_conf.setReducerClass(BankTransferReducer.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		my_client.setConf(job_conf);
        try { 
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
