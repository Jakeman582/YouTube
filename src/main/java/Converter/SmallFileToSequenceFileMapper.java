package Converter;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SmallFileToSequenceFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// We want to get the YouTuber's/file name to use as a key
	private Text newKey;
	
	// Extract the YouTuber's name from the current input split
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		InputSplit split = context.getInputSplit();
		Path path = ((FileSplit) split).getPath();
		
		// Remove the .txt extension from the file name
		newKey = new Text(path.getName().split(".")[0]);
	}
	
	// Start processing each YouTube video's metadata
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Extract each part of the video's metadata
		String[] parts = value.toString().split(" \\| ", 5);
		String views = parts[0];
		String comments = parts[3];
		String likes = parts[1];
		String dislikes = parts[2];
		
		// Clean the video title
		String title = parts[4]
				.toLowerCase()
				.replaceAll("[_/]+", " ")
				.replaceAll("[^\\x61-\\x7A]+", "");
		
		// Output results to the SequenceFile
		Text newValue = new Text(
			views		+ "\t" + 
			comments	+ "\t" +
			likes		+ "\t" + 
			dislikes	+ "\t" +
			title
		);
		context.write(newKey, newValue);
		
	}

}
