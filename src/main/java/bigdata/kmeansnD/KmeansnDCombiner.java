package bigdata.kmeansnD;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansnDCombiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double sum = 0L;
		int nb_points = 0;
		for (DoubleWritable v : values) {
			sum += v.get();
			++nb_points;
		}
		double new_point = sum / nb_points;
		context.write(key, new DoubleWritable(new_point));
	}
}
