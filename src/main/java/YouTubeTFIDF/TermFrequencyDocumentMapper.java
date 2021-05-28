package YouTubeTFIDF;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utilities.Parser;

public class TermFrequencyDocumentMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	/**
	 * Parses out vital information for calculating how many times a term t
	 * appears in video titles v for YouTuber y.
	 * 
	 * This method expects to receive a key (type:Text) containing the YouTuber's name.
	 * This method expects to receive a value (type:Text) with the following format:
	 * <views | comments | likes | dislikes | title>.
	 * 
	 * @param key the YouTuber that published the video in question
	 * @param value the string representation of a particular YouTube video
	 * @param context the Map context where output will be written
	 * @see Mapper
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		
		
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
