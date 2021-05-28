package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermFrequencyCollectionMapper extends Mapper<Text, Text, Text, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		// Extract the term and document from the key
		String[] keys = key.toString().split("\t");
		String term = keys[0];
		String document = keys[1];
		
		// Extract the term count, and total word count from the value
		String[] values = value.toString().split("\t");
		String termCount = values[0];
		String totalWordCount = values[1];
		
		// Form the value to send to the reducer
		String newValue = document + "\t" + termCount + "\t" + totalWordCount + "\t1";
		
		// Send to the reducer
		context.write(
			new Text(term),
			new Text(newValue)
		);
		
	}
	
}
