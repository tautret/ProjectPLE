package bigdata.kmeans2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

public class KmeansAlgo extends Configured implements Tool {

	static HashMap<IntWritable, LongWritable> pivots = new HashMap<IntWritable, LongWritable>();

	public static final long measureDistance(long pivot, long point) {
		long sum = 0;
		sum = Math.abs(pivot - point);
		return sum;
	}

	public static class KmeansMapper extends Mapper<IntWritable, LongWritable, IntWritable, LongWritable> {
		protected void setup(Context context) throws IOException {
			/* Recovery pivots in the Distributed Cache */
			URI[] files = context.getCacheFiles();
			Configuration conf = context.getConfiguration();
			Path path = new Path(files[0]);
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(conf, Reader.file(path));
				IntWritable key_cache = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				LongWritable value_cache = (LongWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

				while (reader.next(key_cache, value_cache)) {
					pivots.put(key_cache, value_cache);
				}
			} finally {
				IOUtils.closeStream(reader);
			}
		}

		public void map(IntWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
			Long min = Long.MAX_VALUE;
			IntWritable pivot = null;
			for (Map.Entry<IntWritable, LongWritable> entry : pivots.entrySet()) {
				long distance = measureDistance(entry.getValue().get(), value.get());
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
			context.write(pivot, value);
		}
	}

	public static class KmeansReducer extends Reducer<IntWritable, Iterable<LongWritable>, IntWritable, LongWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
		}
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
