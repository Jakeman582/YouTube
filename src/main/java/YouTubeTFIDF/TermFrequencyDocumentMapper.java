package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utilities.Parser;

public class TermFrequencyDocumentMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		
		// The key from the SequenceFile is the YouTube channel, and will
		// be used as the name of the document, we just need the words of 
		// the title
		String document = key.toString();
		String[] words = Parser.parseTitle(value);
		
		// For each of the words, set an increment and send to the reducer
		for(String word : words) {
			context.write(
					new Text(word + "\t" + document),
					new IntWritable(1)
			);
		}
		
	}
	
}
