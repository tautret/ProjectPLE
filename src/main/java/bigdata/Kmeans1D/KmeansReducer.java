package bigdata.Kmeans1D;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansReducer extends
		Reducer<IntWritable, Iterable<FormatPivot>, Text, IntWritable> {
	Writer writer;

	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path path_out = new Path(context.getConfiguration().get(
				"path new pivot"));
		FileSystem fs = FileSystem.get(conf);

		writer = SequenceFile.createWriter(conf, Writer.file(path_out), Writer
				.keyClass(IntWritable.class), Writer
				.valueClass(DoubleWritable.class), Writer.bufferSize(fs
				.getConf().getInt("io.file.buffer.size", 4096)), Writer
				.replication(fs.getDefaultReplication(path_out)), Writer
				.blockSize(1073741824), Writer.compression(
				SequenceFile.CompressionType.BLOCK, new DefaultCodec()), Writer
				.progressable(null), Writer.metadata(new Metadata()));
	}

	public void reduce(IntWritable key, Iterable<FormatPivot> values,
			Context context) throws IOException, InterruptedException {
		List<FormatPivot> points = new ArrayList<FormatPivot>();
		double sum = 0L;
		int nb_points = 0;
		for (FormatPivot v : values) {
			sum += v.getPoint();
			++nb_points;
			points.add(v);
			context.write(new Text(v.getText()), key);
		}
		double new_point = sum / nb_points;
		writer.append(key, new_point);
	}

	protected void cleanup(Context context) throws IOException {
		writer.close();
	}
}