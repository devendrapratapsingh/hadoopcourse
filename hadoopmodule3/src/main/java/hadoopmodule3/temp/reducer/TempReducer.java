package hadoopmodule3.temp.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TempReducer extends MapReduceBase implements
Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	

	public void reduce( IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		List<Integer> allTempofYear=new ArrayList<Integer>();
		
		while(values.hasNext()){
			allTempofYear.add(values.next().get());
		}
		output.collect(key, new IntWritable(Collections.max(allTempofYear)));
	}

}
