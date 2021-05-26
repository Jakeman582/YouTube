package YouTubeTFIDF;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentTermCountReducer extends Reducer<Text, Text, Text, Text>{
	
	private final int TERM = 0;
	private final int COUNT = 1;
	
	public void reduce(Text document, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for(Text count : counts) {
			String[] parts = count.toString().split("\t");
			sum += Integer.parseInt(parts[COUNT]);
		}
		
		for(Text count : counts) {
			String[] parts = count.toString().split("\t");
			context.write(
				new Text(parts[TERM] + "\t" + document.toString()),
				new Text(parts[COUNT] + "\t" + sum)
			);
		}
		
	}
	
}
