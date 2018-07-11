import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.FloatWritable;
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

public class PageFormatter {
	public static class FormatMapper extends Mapper <Object, Text, FloatWritable, Text> {
		private FloatWritable keyInfo = new FloatWritable();
		private Text valueInfo = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			int idx = str.indexOf("\t");
			int idy = str.indexOf(";", idx);
			if (idy == -1) {
				idy = str.length();
			}
			if (idx == -1 || idy == -1 || idx >= idy) {
				return;
			}
			String page = str.substring(0, idx);
			String rankstr = str.substring(idx + 1, idy);

			Float rank = Float.valueOf(rankstr);

			keyInfo.set(rank);
			valueInfo.set(page);
			context.write(keyInfo, valueInfo);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "page format");
		job.setJarByClass(PageFormatter.class);
		
		job.setMapperClass(FormatMapper.class);

		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("wiki-num/iter10"));
		FileOutputFormat.setOutputPath(job, new Path("wiki-num/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
