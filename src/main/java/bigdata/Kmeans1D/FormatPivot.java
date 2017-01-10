package bigdata.Kmeans1D;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FormatPivot implements Writable {

	private double point;
	private String text;
	
	public FormatPivot(){}

	public FormatPivot(double _point, String _text) {
		this.point = _point;
		this.text = _text;
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(point);
		out.writeChars(text);

	}

	public void readFields(DataInput in) throws IOException {
		point = in.readDouble();
		text = in.readLine();
	}

	public double getPoint() {
		return point;
	}

	public String getText() {
		return text;
	}

}
