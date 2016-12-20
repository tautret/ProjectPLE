/**
 * @author David Auber 
 * @date 07/10/2016
 * Ma�tre de conf�rences HDR
 * LaBRI: Universit� de Bordeaux
 */
 package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class BigData {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("ChoosePivot", bigdata.kmeans2.ChoosePivot.class, "ChoosePivot");
			pgd.addClass("Tpinput", bigdata.io.TPInputFormat.class, "tpinput");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
