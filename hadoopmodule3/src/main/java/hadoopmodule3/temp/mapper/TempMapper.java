package hadoopmodule3.temp.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TempMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		String[] record=value.toString().split(" ");
					
			output.collect(new IntWritable(Integer.valueOf(record[0])), new IntWritable(Integer.valueOf(record[1])));
	
		
	}

}
