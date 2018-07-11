import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRanker {
	public static class PageMapper extends Mapper <Object, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			int idx = str.indexOf("\t");
			int idy = str.indexOf(";", idx);
			if (idx == -1 || idy == -1) {
				return;
			}
			String page = str.substring(0, idx);
			String rank = str.substring(idx + 1, idy);
			String linkstr = str.substring(idy + 1);

			if (linkstr == "") {
				return;
			}
			String[] links = linkstr.split(",");
			for (String link : links) {
				keyInfo.set(link);
				valueInfo.set(page + ":" + rank + "," + links.length);
				context.write(keyInfo, valueInfo);
			}
			keyInfo.set(page);
			valueInfo.set("|" + linkstr);
			context.write(keyInfo, valueInfo);
		}
	}
	
	public static class PageReducer extends Reducer <Text, Text, Text, Text> {
		private Text res = new Text();
		private static final float damping = 0.85F;

		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			String str = "";
			String linkstr = "";
			float tmp = 0;

			for (Text value : values) {
				str = value.toString();
				if (str.startsWith("|")) {
					linkstr = str.substring(1);
					continue;
				}
				int idx = str.indexOf(":");
				int idy = str.indexOf(",", idx);
				float rank = Float.valueOf(str.substring(idx + 1, idy));
				int cnt = Integer.valueOf(str.substring(idy + 1));
				tmp += rank / cnt;
			}

			float rank = damping * tmp + (1 - damping);
			res.set(String.valueOf(rank) + ";" + linkstr);
			context.write(key, res);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "page rank");
		job.setJarByClass(PageRanker.class);
		
		job.setMapperClass(PageMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setReducerClass(PageReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("wiki-num/iter" + args[0]));
		FileOutputFormat.setOutputPath(job, new Path("wiki-num/iter" + args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
