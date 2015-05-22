package hadoopmodule3.patent.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PatentMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
			Reporter reporter) throws IOException {
		String[] record=value.toString().split(" ");
		if(record[1].indexOf(record[0]+".")!=1){
			
			output.collect(new Text(record[1]), new LongWritable(1));
		}
		
	}

}
