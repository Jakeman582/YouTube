package Utilities;

import org.apache.hadoop.io.Text;

/**
 * A collection of static methods for parsing out YouTube video 
 * metadata.
 * 
 * @author jacobhiance
 *
 */
public class Parser {
	
	// There are 5 pieces of metadata for each YouTube video
	private static final int DATA_ITEMS = 5;
	
	/* Parse out the video titles from the input SequenceFile */
	public static String[] parseTitle(Text input) {
		
		// Get the words in the video title
		String[] videoData = input.toString().split(" \\| ", DATA_ITEMS);
		String[] words = videoData[DATA_ITEMS - 1].split("[\\s_/]+");
		
		// For each word
		for(String word : words) {
			
			// Make sure all characters are lower-case ASCII
			word = word.toLowerCase();
			word = word.replaceAll("[^\\x61-\\x7A]+", "");
		}
		
		return words;
		
	}
	
}
