package bigdata.kmeansnD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KmeansAlgo extends Configured implements Tool {

	static HashMap<IntWritable, DoubleWritable> center = new HashMap<IntWritable, DoubleWritable>();

	boolean isChanged = true;

	public static final double measureDistance(double pivot, double point) {
		return Math.abs(pivot - point);
	}

	public void ChooseNbPivot(Configuration c, Path path_in, Path path_out,
			int nb_pivots, int num_colonne) throws IOException,
			InterruptedException, URISyntaxException {

		int num = 0;
		URI uri = path_in.toUri();
		FileSystem fs = FileSystem.get(uri, c, "hadoop");
		BufferedReader file_in = new BufferedReader(new InputStreamReader(
				fs.open(path_in)));
		SequenceFile.Writer writer = SequenceFile.createWriter(c, Writer
				.file(path_out), Writer.keyClass(IntWritable.class), Writer
				.valueClass(DoubleWritable.class), Writer.bufferSize(fs
				.getConf().getInt("io.file.buffer.size", 4096)), Writer
				.replication(fs.getDefaultReplication(path_out)), Writer
				.blockSize(1073741824), Writer.compression(
				SequenceFile.CompressionType.BLOCK, new DefaultCodec()), Writer
				.progressable(null), Writer.metadata(new Metadata()));

		HashSet<Double> pivots = new HashSet<Double>();
		for (int i = 0; i < nb_pivots; i++) {
			try {
				String line = file_in.readLine();
				String tokens[] = line.split(",");
				double point = Double.parseDouble(tokens[num_colonne]);
				if (!pivots.add(point)) {
					i--;
				}
			} catch (NumberFormatException e) {
				i--;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		for (Double s : pivots) {
			center.put(new IntWritable(num),
					new DoubleWritable(s.doubleValue()));
			writer.append(new IntWritable(num),
					new DoubleWritable(s.doubleValue()));
			System.out.println("Num pivot : " + num + " point : "
					+ s.doubleValue());
			num++;
		}

		writer.close();

	}

	public int run(String args[]) throws Exception {
		Path in, out, pivots, tmp_out;
		int nb_pivot, nb_colonne;
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		try {
			in = new Path(args[0]);
			out = new Path(args[1]);
			nb_pivot = Integer.parseInt(args[2]);
			nb_colonne = Integer.parseInt(args[3]);
			tmp_out = new Path(args[1] + "_tmp.txt");
			pivots = new Path(args[1] + "_pivots.txt");
			//conf.set("path pivot", pivots.toString());
			//conf.set("path tmp out", tmp_out.toString());
			conf.setInt("num_col", nb_colonne);
		} catch (Exception e) {
			System.out
					.println(" bad arguments, waiting for 4 arguments [inputURI] [outputURI][NB_PIVOTS][NUM_COL]");
			return -1;
		}

		ChooseNbPivot(conf, in, pivots, nb_pivot, nb_colonne);

		while (isChanged) {
			fs.delete(out, true);
			Job job = Job.getInstance(conf, "Kmeans Algo");
			job.addCacheFile(pivots.toUri());
			job.setNumReduceTasks(1);
			job.setJarByClass(KmeansAlgo.class);
			job.setMapperClass(KmeansMapper.class);
			job.setCombinerClass(KmeansCombiner.class);
			job.setReducerClass(KmeansReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, tmp_out);
			job.waitForCompletion(true);
			FileUtil.copyMerge(fs, tmp_out, fs, out, true, conf, "");
			isDone(out);
			fs.delete(pivots, true);
			fs.rename(out, pivots);

		}

		Job job = Job.getInstance(conf, "Merge file");
		job.setNumReduceTasks(1);
		job.addCacheFile(pivots.toUri());
		job.setJarByClass(KmeansAlgo.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, tmp_out);
		job.waitForCompletion(true);
		FileUtil.copyMerge(fs, tmp_out, fs, out, true, conf, "");
		return 0;
	}

	public void isDone(Path out) throws IOException {
		HashMap<IntWritable, DoubleWritable> tmp_center = new HashMap<IntWritable, DoubleWritable>();

		Configuration conf = getConf();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,
				Reader.file(out), Reader.bufferSize(4096), Reader.start(0));

		IntWritable key = new IntWritable();
		DoubleWritable value = new DoubleWritable();
		double sum_newCenter = 0;
		double sum_oldCenter = 0;
		while (reader.next(key, value)) {
			System.out.println("Num :" + key.toString() + " Pivots :"
					+ value.toString());
			tmp_center.put(key, value);
			sum_newCenter += value.get();
			key = new IntWritable();
			value = new DoubleWritable();
		}
		for (Map.Entry<IntWritable, DoubleWritable> d : center.entrySet()) {
			sum_oldCenter += d.getValue().get();
		}
		double sum = sum_oldCenter - sum_newCenter;
		if (sum <= 0.1 && sum >= -0.1 ) {
			isChanged = false;
		}

		center = tmp_center;
		IOUtils.closeStream(reader);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new KmeansAlgo(), args);
	}

}
