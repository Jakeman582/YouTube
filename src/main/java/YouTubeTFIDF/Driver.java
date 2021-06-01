package YouTubeTFIDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A driver for performing a TF-IDF analysis on a YouTuber's video 
 * titles.
 * 
 * @author jacobhiance
 *
 */
public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		// We want to run 5 jobs using the same configuration
		Configuration config = new Configuration();
		
		// Configure the document count job
		Job job = Job.getInstance(config);
		job.setJarByClass(Driver.class);
		job.setJobName("Counting the number of YouTubers");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("/temp/TFIDF_1"));
		job.setMapperClass(DocumentCountMapper.class);
		job.setReducerClass(DocumentCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return 0;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Driver(), args));
	}

}
