package aws.emr.ipcount;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class IPCountMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text time = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		Pattern p = Pattern.compile("(?:^|[\\s\\.;\\?\\!,])[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+(?:$|[\\s\\.;\\?\\!,])");
		while (tokenizer.hasMoreTokens()) {
			time.set(tokenizer.nextToken());
			Matcher m = p.matcher(time.toString());
			if(m.matches()) {
				output.collect(time, one);
			}
		}
	}
}