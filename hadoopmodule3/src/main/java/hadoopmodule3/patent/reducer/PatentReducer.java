package hadoopmodule3.patent.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PatentReducer extends MapReduceBase implements
Reducer<Text, LongWritable, Text, Text> {

	

	public void reduce(Text key, Iterator<LongWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		long sum=0;
		
		while(values.hasNext()){
			sum+=values.next().get();
		}
		output.collect(key, new Text(String.valueOf(sum)));
	}

}
