package hadoopmodule3.temp;

import hadoopmodule3.temp.mapper.TempMapper;
import hadoopmodule3.temp.reducer.TempReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TempJob {

	public static void main(String args[]) throws IOException {
		JobConf job = new JobConf(TempJob.class);
		job.setJobName("Temp");
		job.setMapperClass(TempMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(TempReducer.class);
		job.setReducerClass(TempReducer.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
	}
}
