package YouTubeTFIDF;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentCountReducer extends Reducer<NullWritable, Text, NullWritable, NullWritable> {
	
	// Keep track of how many documents/YouTubers have been seen
	int documentCount;
	
	// Count the number of documents/YouTubers seen
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		documentCount = 0;
		for(Text value : values) {
			documentCount++;
		}
	}
	
	// Write the number of documents seen to the distributed cache
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.getConfiguration().set("DOCUMENT_COUNT", "" + documentCount);
	}
	
}
