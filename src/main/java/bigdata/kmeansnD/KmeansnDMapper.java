package bigdata.kmeansnD;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

public class KmeansnDMapper extends Mapper<LongWritable, Text, IntWritable, FormatPivot> {
	static HashMap<IntWritable, FormatPivot> center = new HashMap<IntWritable, FormatPivot>();

	protected void setup(Context context) throws IOException {
		/* Recovery pivots in the Distributed Cache */
		URI[] files = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		Path path = new Path(files[0]);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
			IntWritable key_cache = new IntWritable();
			FormatPivot value_cache = new FormatPivot();
			while (reader.next(key_cache, value_cache)) {
				center.put(key_cache, value_cache);
				key_cache = new IntWritable();
				value_cache = new FormatPivot();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		int n_dim = context.getConfiguration().getInt("n dimensions", 0);
		int[] colonne = new int[n_dim];
		for (int i = 0; i < n_dim; i++) {
			colonne[i] = context.getConfiguration().getInt(String.valueOf(i) + " dimension", 0);
		}
		String tokens[] = value.toString().split(",");
		Double best_dist = null;
		IntWritable best_pivot = null;
		List<Double> tmp_list = new ArrayList<Double>(n_dim);
		
		try {
			tmp_list = new ArrayList<Double>(n_dim);
			for (int x = 0; x < n_dim; x++) {
				tmp_list.add(Double.parseDouble(tokens[colonne[x]]));
			}
			for (IntWritable p : center.keySet()) {
				FormatPivot formatPivot = center.get(p);
				Double distance = KmeansnD.measureDistance(formatPivot.getList_point(), tmp_list);
				if (best_pivot == null || distance < best_dist) {
					best_dist = distance;
					best_pivot = p;
				}
			}
			context.write(best_pivot, new FormatPivot(tmp_list,n_dim));
			
		} catch (NumberFormatException e) {
		} catch (Exception e) {
		}
	}

}
