package bigdata.kmeansnD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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

public class KmeansnD extends Configured implements Tool {

	static HashMap<IntWritable, FormatPivot> center = new HashMap<IntWritable, FormatPivot>();

	boolean isChanged = true;

	public static final Double measureDistance(List<Double> point1, List<Double> point2) {
		double somme = 0;
		for (int i = 0; i < point1.size(); i++) {
			somme += point1.get(i) - point2.get(i);
		}
		return Math.abs(somme);
	}

	public void ChooseNbPivot(Configuration c, Path path_in, Path path_out, int nb_pivots, int[] nb_colonne)
			throws IOException, InterruptedException, URISyntaxException {

		int num = 0;
		URI uri = path_in.toUri();
		FileSystem fs = FileSystem.get(uri, c, "hadoop");
		BufferedReader file_in = new BufferedReader(new InputStreamReader(fs.open(path_in)));
		SequenceFile.Writer writer = SequenceFile.createWriter(c, Writer.file(path_out),
				Writer.keyClass(IntWritable.class), Writer.valueClass(FormatPivot.class),
				Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
				Writer.replication(fs.getDefaultReplication(path_out)), Writer.blockSize(1073741824),
				Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()), Writer.progressable(null),
				Writer.metadata(new Metadata()));

		HashSet<List<Double>> pivots = new HashSet<List<Double>>();
		for (int i = 0; i < nb_pivots; i++) {
			try {
				String line = file_in.readLine();
				String tokens[] = line.split(",");
				List<Double> tmp = new ArrayList<Double>();
				for (int x = 0; x < nb_colonne.length; x++) {
					tmp.add(Double.parseDouble(tokens[nb_colonne[x]]));
				}
				if (!pivots.add(tmp)) {
					i--;
				}
			} catch (NumberFormatException e) {
				i--;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		for (List<Double> tmpList : pivots) {
			FormatPivot tmp = new FormatPivot(tmpList, nb_colonne.length);
			center.put(new IntWritable(num), tmp);
			writer.append(new IntWritable(num), tmp);
			num++;
		}
		writer.close();
	}

	public int run(String args[]) throws Exception {
		Path in, out, pivots, tmp_out;
		int nb_pivot;
		int[] nb_colonne = new int[args.length - 3];
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		try {
			in = new Path(args[0]);
			out = new Path(args[1]);
			nb_pivot = Integer.parseInt(args[2]);
			int i = 0;
			for (int x = 3; x <= args.length - 1; x++) {
				nb_colonne[i] = Integer.parseInt(args[x]);
				i++;
			}
			for (int y = 0; y < nb_colonne.length; y++) {
				conf.setInt(y + " dimension", nb_colonne[y]);
			}
			conf.setInt("n dimensions", nb_colonne.length);
			tmp_out = new Path(args[1] + "_tmp.txt");
			pivots = new Path(args[1] + "_pivots.txt");
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		ChooseNbPivot(conf, in, pivots, nb_pivot, nb_colonne);

		while (isChanged) {
			fs.delete(out, true);
			Job job = Job.getInstance(conf, "Kmeans Algo");
			job.addCacheFile(pivots.toUri());
			job.setNumReduceTasks(1);
			job.setJarByClass(KmeansnD.class);
			job.setMapperClass(KmeansnDMapper.class);
			job.setCombinerClass(KmeansnDCombiner.class);
			job.setReducerClass(KmeansnDReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(FormatPivot.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(FormatPivot.class);
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
		job.setJarByClass(KmeansnD.class);
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
		HashMap<IntWritable, FormatPivot> tmp_center = new HashMap<IntWritable, FormatPivot>();

		Configuration conf = getConf();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(out), Reader.bufferSize(4096),
				Reader.start(0));

		IntWritable key = new IntWritable();
		FormatPivot value = new FormatPivot();
		double sum_newCenter = 0;
		double sum_oldCenter = 0;
		System.out.println("Nouveaux pivots :");
		while (reader.next(key, value)) {
			System.out.println("Numéro Pivot :" + key.toString() + " Points :" + value.getListToString());
			tmp_center.put(key, value);
			for (Double points : value.getList_point()) {
				sum_newCenter += points.doubleValue();
			}
			key = new IntWritable();
			value = new FormatPivot();
		}
		for (Map.Entry<IntWritable, FormatPivot> d : center.entrySet()) {
			System.out.println("Numéro Pivot :" + d.getKey().toString() + " Point :" + d.getValue().getListToString());
			for (Double p : d.getValue().getList_point()) {
				sum_oldCenter += p.doubleValue();
			}
		}

		double sum = sum_oldCenter - sum_newCenter;
		if (sum <= 0.1 && sum >= -0.1) {
			isChanged = false;
		}

		center = tmp_center;
		IOUtils.closeStream(reader);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new KmeansnD(), args);
	}

}
