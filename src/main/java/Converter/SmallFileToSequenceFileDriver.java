package Converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFileToSequenceFileDriver extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 2)  {
			System.out.println("Usage: SmallFileToSequenceFileConverter <input_file> <output_directory>");
			System.exit(0);
		}
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(SmallFileToSequenceFileDriver.class);
		job.setJobName("Small file to SequenceFile Converter");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(SmallFileToSequenceFileMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		SequenceFileOutputFormat.setCompressOutput(job, false);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SmallFileToSequenceFileDriver(), args));
	}

}
