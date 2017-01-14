package bigdata.kmeans1D;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Kmeans1DMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	static HashMap<IntWritable, DoubleWritable> center = new HashMap<IntWritable, DoubleWritable>();

	protected void setup(Context context) throws IOException {
		/* Recovery pivots in the Distributed Cache */
		URI[] files = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		Path path = new Path(files[0]);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path),
					Reader.bufferSize(4096), Reader.start(0));
			IntWritable key_cache = new IntWritable();
			DoubleWritable value_cache = new DoubleWritable();
			while (reader.next(key_cache, value_cache)) {
				center.put(key_cache, value_cache);
				key_cache = new IntWritable();
				value_cache = new DoubleWritable();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int num_col = context.getConfiguration().getInt("num_col", 0);
		String tokens[] = value.toString().split(",");
		Double best_dist = null;
		IntWritable best_pivot = null;
		try {
			double point = Double.parseDouble(tokens[num_col]);
			for (IntWritable p : center.keySet()) {
				DoubleWritable tmp_point = center.get(p);
				Double distance = Kmeans1D.measureDistance(tmp_point.get(),point);
				if (best_pivot == null || distance <= best_dist) {
					best_dist = distance;
					best_pivot = p;
				}
			}
			context.write(best_pivot, new DoubleWritable(point));
		} catch (NumberFormatException e) {
		} catch (Exception e) {
		}
		// TODO: handle exception
	}

}
