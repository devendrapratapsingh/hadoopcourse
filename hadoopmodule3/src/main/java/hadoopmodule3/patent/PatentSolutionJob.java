package hadoopmodule3.patent;

import hadoopmodule3.patent.mapper.PatentMapper;
import hadoopmodule3.patent.reducer.PatentReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PatentSolutionJob {
	
public static void main(String args[]) throws IOException{
	JobConf job=new JobConf(PatentSolutionJob.class);
	job.setJobName("Patent");
	job.setMapperClass(PatentMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setCombinerClass(PatentReducer.class);
	job.setReducerClass(PatentReducer.class);
	
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
JobClient.runJob(job);
}
}
