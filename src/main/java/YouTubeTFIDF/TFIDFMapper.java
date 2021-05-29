package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	
	private int numberOfDocuments;	// Total number of YouTubers considered
	
	// Extract the number of documents from distributed cache
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		Configuration config = context.getConfiguration();
		numberOfDocuments = Integer.parseInt(config.get("NUMBER_OF_DOCUMENTS"));
		
	}
	
	// Calculate the TF-IDF for each term with regards to each YouTuber
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		// Extract each of the calculated components from the value
		String[] parts = value.toString().split("\t");
		int termDocumentFrequency = Integer.parseInt(parts[0]);
		int documentWordCount = Integer.parseInt(parts[1]);
		int termCollectionFrequency = Integer.parseInt(parts[2]);
		
		// Calculate the TF-IDF for the (term, document) pair
		double termFrequency = termDocumentFrequency / documentWordCount;
		double inverseDocumentFrequency = numberOfDocuments / termCollectionFrequency;
		double tfidf = termFrequency * Math.log10(inverseDocumentFrequency);
		
		// Output the results
		context.write(key, new DoubleWritable(tfidf));
		
	}
	
}
