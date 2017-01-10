package bigdata.kmeans2;

import org.apache.hadoop.util.ProgramDriver;

public class Project {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("kmeans", bigdata.kmeans2.KmeansAlgo.class, "kmeans");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}