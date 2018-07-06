import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static class InvMapper extends Mapper <Object, Text, Text, Text> {
		private FileSplit split;
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			split = (FileSplit) context.getInputSplit();
			String pathname = split.getPath().toString();
			int idx = pathname.lastIndexOf("/");
			String filename = pathname.substring(idx + 1);
			int base = (int) split.getStart();

			String str = value.toString();
			Pattern pattern = Pattern.compile("[^a-z|A-Z]");
			Matcher matcher = pattern.matcher(str + ".");
			int lastOff = 0, curOff = 0;
			while (matcher.find()) {
				curOff = matcher.start();
				if (curOff - lastOff > 0) {
					String word = str.substring(lastOff, curOff).toLowerCase();
					int add = base + lastOff;
					keyInfo.set(word + ":" + filename);
					valueInfo.set(String.valueOf(base));
					context.write(keyInfo, valueInfo);
				}
				lastOff = curOff + 1;
			}
		}
	}

	public static class InvCombiner extends Reducer <Text, Text, Text, Text> {
		private Text info = new Text();

		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer buf = new StringBuffer();
			for (Text value : values) {
				buf.append(value.toString() + ",");
			}
			int idx = key.toString().indexOf(":");
			String word = key.toString().substring(0, idx);
			String filename = key.toString().substring(idx + 1);
			info.set(filename + ":" + buf.substring(0, buf.length() - 1));
			key.set(word);
			context.write(key, info);
		}
	}

	public static class InvReducer extends Reducer <Text, Text, Text, Text> {
		private Text res = new Text();
		
		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer buf = new StringBuffer();
			for (Text value : values) {
				buf.append(value.toString() + ";");
			}
			res.set(buf.substring(0, buf.length() - 1));
			context.write(key, res);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setCombinerClass(InvCombiner.class);
		job.setReducerClass(InvReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
