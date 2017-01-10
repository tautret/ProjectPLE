package bigdata.kmeans2;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KmeansMapper extends Mapper<IntWritable, Text, IntWritable, FormatPivot> {
	protected void setup(Context context) throws IOException {
		/* Recovery pivots in the Distributed Cache */
		URI[] files = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		Path path = new Path(files[0]);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
			IntWritable key_cache = new IntWritable();
			DoubleWritable value_cache = new DoubleWritable();
			while (reader.next(key_cache, value_cache)) {
				KmeansAlgo.center.put(key_cache, value_cache);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		double min = Double.MAX_VALUE;
		IntWritable pivot = null;
		int num_col = context.getConfiguration().getInt("num_col", 0);
		String tokens[] = value.toString().split(",");
		double point = Double.parseDouble(tokens[num_col]);
		for (Map.Entry<IntWritable, DoubleWritable> entry : KmeansAlgo.center.entrySet()) {
			double distance = KmeansAlgo.measureDistance(entry.getValue().get(), point);
			if (pivot == null) {
				pivot = entry.getKey();
				min = distance;
			} else {
				if (min > distance) {
					min = distance;
					pivot = entry.getKey();
				}
			}
		}
		context.write(pivot, new FormatPivot(point, value.toString()));
	}
}