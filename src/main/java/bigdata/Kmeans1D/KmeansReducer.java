package bigdata.Kmeans1D;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends Reducer<IntWritable, FormatPivot, IntWritable, DoubleWritable> {


	public void reduce(IntWritable key, Iterable<FormatPivot> values, Context context) throws IOException, InterruptedException {
		double sum = 0L;
		int nb_points = 0;
		for (FormatPivot v : values) {
			sum += v.getPoint();
			++nb_points;
		}
		double new_point = sum / nb_points;
		context.write(key,new DoubleWritable(new_point));
	}

}