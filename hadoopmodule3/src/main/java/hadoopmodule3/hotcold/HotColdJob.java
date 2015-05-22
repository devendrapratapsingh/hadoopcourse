package hadoopmodule3.hotcold;

import hadoopmodule3.hotcold.mapper.HotColdMapper;
import hadoopmodule3.hotcold.partitioner.HotColdPartioner;
import hadoopmodule3.hotcold.reducer.HotColdReducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class HotColdJob {

	public static void main(String args[]) throws IOException {
		JobConf job = new JobConf(HotColdJob.class);
		job.setJobName("HotCold");
		job.setNumReduceTasks(3);
		job.setMapperClass(HotColdMapper.class);
		job.setPartitionerClass(HotColdPartioner.class);
		job.setReducerClass(HotColdReducer.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(job).delete(outputPath);
		JobClient.runJob(job);
	}
}
