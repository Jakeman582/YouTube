package YouTubeTFIDF;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentCountReducer extends Reducer<NullWritable, Text, NullWritable, NullWritable> {
	
	// Keep track of how many documents/YouTubers have been seen
	Set<Text> documents;
	
	// Count the number of documents/YouTubers seen
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		documents = new HashSet<Text>();
		for(Text value : values) {
			documents.add(value);
		}
	}
	
	// Write the number of documents seen to the distributed cache
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.getConfiguration().set("DOCUMENT_COUNT", "" + documents.size());
	}
	
}
