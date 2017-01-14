package bigdata.kmeans1D;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text v : values){
			String line = v + "," + key.toString();
			context.write(null,new Text(line));
			line = "";
		}
	}
}
