package hadoopmodule3.wordlength;

import hadoopmodule3.wordlength.mapper.WordLengthMapper;
import hadoopmodule3.wordlength.reducer.WordLengthReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordLengthJob {
	
public static void main(String args[]) throws IOException{
	JobConf job=new JobConf(WordLengthJob.class);
	job.setJobName("WordLength");
	
	job.setMapperClass(WordLengthMapper.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setCombinerClass(WordLengthReducer.class);
	job.setReducerClass(WordLengthReducer.class);
	
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(LongWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
JobClient.runJob(job);
}
}
