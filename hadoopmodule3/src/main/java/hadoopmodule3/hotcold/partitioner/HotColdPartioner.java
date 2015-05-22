package hadoopmodule3.hotcold.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class HotColdPartioner implements Partitioner<Text, Text> {

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	public int getPartition(Text key, Text value, int partitionNum) {
		if (key.toString().equals("Hot")) {
			return 0;
		} else if (key.toString().equals("Cold")) {
			return 1;
		} else {
			return 2;
		}
	}

}
