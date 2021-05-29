package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentTermCountMapper extends Mapper<Text, IntWritable, Text, Text>{
	
	private final int WORD = 0;
	private final int DOCUMENT = 1;
	
	public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		
		// Extract the term and YouTuber/document name from the key
		String[] parts = key.toString().split("\t");
		
		// Output to the reducer
		context.write(
			new Text(parts[DOCUMENT]),
			new Text(parts[WORD] + "\t" + value.get())
		);
		
	}
	
}
