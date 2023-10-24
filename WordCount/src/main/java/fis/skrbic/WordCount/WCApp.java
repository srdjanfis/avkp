package fis.skrbic.WordCount;

/**
 * Hello world!
 *
 */
import java.io.IOException;    
import org.apache.hadoop.fs.Path;    
import org.apache.hadoop.io.IntWritable;    
import org.apache.hadoop.io.Text;    
import org.apache.hadoop.mapred.FileInputFormat;    
import org.apache.hadoop.mapred.FileOutputFormat;    
import org.apache.hadoop.mapred.JobClient;    
import org.apache.hadoop.mapred.JobConf;    
import org.apache.hadoop.mapred.TextInputFormat;    
import org.apache.hadoop.mapred.TextOutputFormat;    
public class WCApp {    
    public static void main(String[] args) throws IOException{    
        JobConf conf = new JobConf(WCApp.class);    
        conf.setJobName("WordCount");    
        conf.setOutputKeyClass(Text.class);    
        conf.setOutputValueClass(IntWritable.class);            
        conf.setMapperClass(WCMap.class);    
            
        conf.setReducerClass(WCRed.class);         
        conf.setInputFormat(TextInputFormat.class);    
        conf.setOutputFormat(TextOutputFormat.class);           
        FileInputFormat.setInputPaths(conf,new Path(args[0]));    
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));     
        JobClient.runJob(conf);    
    }    
}    
