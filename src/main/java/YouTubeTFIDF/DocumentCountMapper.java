package YouTubeTFIDF;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentCountMapper extends Mapper<Text, Text, NullWritable, Text> {
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), key);
	}
	
}
