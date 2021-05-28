package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TermFrequencyCollectionReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// Extract the term from the key
		String term = key.toString();
		
		// Count the number of documents that have the given term
		int sum = 0;
		for(Text value : values) {
			String[] parts = value.toString().split("\t");
			int count = Integer.parseInt(parts[3]);
			sum += count;
		}
		
		// For each (term, document) pair, output the number of times the term appears in any document
		for(Text value : values) {
			
			// Extract each part from the value
			String[] parts = value.toString().split("\t");
			String document = parts[0];
			String termCount = parts[1];
			String totalWordCount = parts[2];
			
			// Output the results
			String newKey = term + "\t" + document;
			String newValue = termCount + "\t" + totalWordCount + "\t" + sum;
			context.write(
				new Text(newKey),
				new Text(newValue)
			);
		}
		
	}
	
}
