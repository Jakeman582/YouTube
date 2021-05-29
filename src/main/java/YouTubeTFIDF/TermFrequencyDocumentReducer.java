package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TermFrequencyDocumentReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		// Count each (term, document) pair to count the number of times
		// each word appears in a YouTuber's video titles
		int count = 0;
		for(IntWritable value : values) {
			count += value.get();
		}
		
		// Write out to the temporary file
		context.write(key, new IntWritable(count));
		
	}
	
}
