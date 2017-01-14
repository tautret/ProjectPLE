package bigdata.kmeansnD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansnDReducer extends Reducer<IntWritable, FormatPivot, IntWritable, FormatPivot> {

	public void reduce(IntWritable key, Iterable<FormatPivot> values, Context context)
			throws IOException, InterruptedException {
		int nb_points = 0;
		int n_dim = context.getConfiguration().getInt("n dimensions", 0);
		List<Double> new_pivots = new ArrayList<Double>(n_dim);
		for (int i = 0; i < n_dim; i++) {
			new_pivots.add(i, new Double(0));
		}
		for (FormatPivot v : values) {
			List<Double> points = v.getList_point();
			for (int i = 0; i < n_dim; i++) {
				new_pivots.set(i, new_pivots.get(i) + points.get(i));
			}
			nb_points++;
		}
		for (int x = 0; x < n_dim; x++) {
			new_pivots.set(x, new_pivots.get(x) / nb_points);
		}
		context.write(key, new FormatPivot(new_pivots, n_dim));

	}
}