package hadoopmodule3.wordlength.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordLengthMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, LongWritable> {

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
			throws IOException {
		StringTokenizer line=new StringTokenizer(value.toString());
		
		while(line.hasMoreTokens()){
			
			output.collect(new IntWritable(line.nextToken().length()), new LongWritable(1));
		}
		
	}

}
