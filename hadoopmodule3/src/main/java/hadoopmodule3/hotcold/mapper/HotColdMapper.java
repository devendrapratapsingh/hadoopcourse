package hadoopmodule3.hotcold.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HotColdMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		StringTokenizer record = new StringTokenizer(value.toString());
		final String[] recordColumns = new String[record.countTokens()];
		int index = 0;
		String dayType = null;
		while (record.hasMoreTokens()) {
			recordColumns[index] = record.nextToken();
			index++;
		}
		if (Float.valueOf(recordColumns[5]) > 40) {
			dayType = "Hot";
		} else if (Float.valueOf(recordColumns[6]) < 10) {
			dayType = "Cold";
		} else {
			dayType = "Normal";
		}
		System.out.println(dayType + "," + recordColumns[1]);
		output.collect(new Text(dayType), new Text(recordColumns[1]));

	}

}
