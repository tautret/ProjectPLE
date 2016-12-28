package bigdata.kmeans2;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

public class ChoosePivot extends Configured implements Tool {
	private Configuration _conf;

	public void ChooseNbPivot(Configuration c, String in, String out, int nb, int num_colonne)
			throws IOException, InterruptedException, URISyntaxException {

		int i = 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path_in = new Path(in);
		Path path_out = new Path(out);
		SequenceFile.Reader reader = null;
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = SequenceFile.createWriter(conf, Writer.file(path_out), Writer.keyClass(key.getClass()),
					Writer.valueClass(value.getClass()),
					Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
					Writer.replication(fs.getDefaultReplication()), Writer.blockSize(1073741824),
					Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
					Writer.progressable(null), Writer.metadata(new Metadata()));
			reader = new SequenceFile.Reader(conf, Reader.file(path_in), Reader.bufferSize(4096), Reader.start(0));
			LongWritable value_read = (LongWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while ((reader.next(key, value)) && i < nb) {
				String tmp[] = value_read.toString().split(",");
				writer.append(new IntWritable(i), tmp[num_colonne]);
				i++;
			}
		} finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(writer);
		}
	}

	public int run(String args[]) throws IOException, InterruptedException, URISyntaxException {

		int nb_ligne = Integer.parseInt(args[2]);
		int num_colonne = Integer.parseInt(args[3]);
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "ChoosePivot");
		ChooseNbPivot(conf, args[0], args[1], nb_ligne, num_colonne);
		job.addCacheFile(new Path(args[1]).toUri());

		return 0;
	}
}
