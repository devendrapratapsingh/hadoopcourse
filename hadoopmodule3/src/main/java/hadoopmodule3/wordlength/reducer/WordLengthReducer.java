package hadoopmodule3.wordlength.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordLengthReducer extends MapReduceBase implements
		Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

	public void reduce(IntWritable key, Iterator<LongWritable> values,
			OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
			throws IOException {
		long wordCounter=0;
		while(values.hasNext()){
			
			wordCounter+=values.next().get();
		}
		
		output.collect(key, new LongWritable(wordCounter));
		
	}

}
