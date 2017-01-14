package bigdata.kmeansnD;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class FormatPivot implements Writable {

	private List<Double> list_point;
	private int n_dim;
	
	public FormatPivot(){}

	public FormatPivot(List<Double> list_point, int n_dim) {
		this.n_dim = n_dim;
		this.list_point = list_point;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(n_dim);
		for(int n = 0; n < n_dim; n++){
			out.writeDouble(list_point.get(n));
		}
	}

	public void readFields(DataInput in) throws IOException {
	    n_dim = in.readInt();
		list_point = new ArrayList<Double>(n_dim);
		for(int i = 0; i < n_dim; i++){
			list_point.add(in.readDouble());
		}
	}

	public List<Double> getList_point() {
		return list_point;
	}
	
	public int getN_dim() {
		return n_dim;
	}

	public String getListToString(){
		String tmp = "";
		for(Double a :list_point){
			tmp += a.toString() + " ";
		}
		return tmp;
	}
	
	public Double sumListPoint(){
		double sum = 0;
		for(int i = 0; i < this.list_point.size(); i++){
			sum += this.list_point.get(i);
		}
		return sum;
	}

}
