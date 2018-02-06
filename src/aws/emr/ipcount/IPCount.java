package aws.emr.ipcount;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class IPCount {

	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(IPCount.class);
		conf.setJobName("IPCount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(IPCountMapper.class);
		conf.setCombinerClass(IPCountReducer.class);
		conf.setReducerClass(IPCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}