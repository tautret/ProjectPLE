package bigdata.kmeans2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;


public class KmeansAlgo extends Configured implements Tool {

	static HashMap<IntWritable, LongWritable> center = new HashMap<IntWritable, LongWritable>();
	static HashMap<IntWritable, LongWritable> old_center = new HashMap<IntWritable, LongWritable>();

	boolean isChanged = true;

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
					center.put(key_cache, value_cache);
				}
			} finally {
				IOUtils.closeStream(reader);
			}
		}

		public void map(IntWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
			Long min = Long.MAX_VALUE;
			IntWritable pivot = null;
			for (Map.Entry<IntWritable, LongWritable> entry : center.entrySet()) {
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

	public static class KmeansReducer
			extends Reducer<IntWritable, Iterable<LongWritable>, Text, Text> {
		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			List<LongWritable> points = new ArrayList<LongWritable>();
			long sum = 0L;
			old_center.get(key).set(center.get(key).get());
			int nb_points = 0;
			for (LongWritable v : values) {
				sum += v.get();
				++nb_points;
				points.add(v);
			}
			long new_point = sum / nb_points;
			center.get(key).set(new_point);
			context.write(new Text(key.toString()), new Text(values.toString()));
		}
	}

	public int run(String args[]) throws Exception {
		while (isChanged == true) {
			Configuration conf = getConf();
			Job job = Job.getInstance(conf,"Kmeans Algo");
			job.setJarByClass(KmeansAlgo.class);
			job.setMapperClass(KmeansMapper.class);
			job.setReducerClass(KmeansReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			try {
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				conf.setInt("steps", Integer.parseInt(args[2]));
			} 
			catch (Exception e) {
				System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [outputURI][NB_STEPS");
				return -1;
			}
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(List.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.waitForCompletion(true);

			Iterator<Map.Entry<IntWritable, LongWritable>> it = center.entrySet().iterator();
			for (Map.Entry<IntWritable, LongWritable> d : old_center.entrySet()) {
				Map.Entry<IntWritable, LongWritable> tmp = it.next();
				if (Math.abs(tmp.getValue().get() - d.getValue().get()) <= 0.1) {
					isChanged = false;
				} else {
					isChanged = true;
					break;
				}
			}
		}
		return 0;
	}

}
