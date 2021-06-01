package YouTubeTitleLister;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A driver for listing all words used in every video title of 
 * every YouTuber. Having a list of words will be useful when 
 * constructing "word count vectors" for every YouTuber.
 * 
 * @author jacobhiance
 *
 */
public class TitleWordLister extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		// Create a new job
		Job job = Job.getInstance();
		job.setJarByClass(TitleWordLister.class);
		job.setJobName("Title Word Counter");
		
		// Set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Import the SequenceFile as text input
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		// This job uses only a mapper
		job.setMapperClass(TitleWordListerMapper.class);
		job.setReducerClass(TitleWordListerReducer.class);
		
		// Specify the output key class and value class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new TitleWordLister(), args);
		System.exit(exitCode);
	}

}