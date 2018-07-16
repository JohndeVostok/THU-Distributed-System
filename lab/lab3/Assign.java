import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;

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

public class Assign {
	
	public static class CanopyCenter {
		public int movieId;
		public Set <Integer> userSet = new HashSet();
	}

	public static class AssignMapper extends Mapper <Object, Text, Text, Text> {
		private List <CanopyCenter> centerList = new ArrayList();
		public static String centerPath = "cluster/canopy/part-r-00000";

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			Set userSet = new HashSet <Integer>();
			StringBuffer buf = new StringBuffer();
			int idx = str.indexOf(":");
			String content = str.substring(idx + 1);
			int movieId = Integer.valueOf(str.substring(0, idx));
			while (idx < str.length() - 1) {
				int idy = str.indexOf(",", idx);
				int idz = str.indexOf(";", idy);
				int userId = Integer.valueOf(str.substring(idx + 1, idy));
				int userRank = Integer.valueOf(str.substring(idy + 1, idz));
				userSet.add(userId);
				idx = idz;
			}

			for (CanopyCenter c : centerList) {
				Set tmp = new HashSet();
				tmp.addAll(c.userSet);
				tmp.retainAll(userSet);
				System.out.println(tmp.size());
				if (tmp.size() >= 2) {
					buf.append(String.valueOf(c.movieId));
					buf.append(",");
				}
			}
			
			buf.append("|");
			buf.append(content);
			Text keyInfo = new Text(String.valueOf(movieId));
			Text valueInfo = new Text(buf.toString());
			context.write(keyInfo, valueInfo);
		}

		@Override
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(new Path(centerPath));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String tmp = null;
			while ((tmp = reader.readLine()) != null) {
				CanopyCenter c = new CanopyCenter();
				int idx = tmp.indexOf("\t");
				int movieId = Integer.valueOf(tmp.substring(0, idx));
				c.movieId = movieId;
				while (idx < tmp.length() - 1) {
					int idy = tmp.indexOf(",", idx + 1);
					int idz = tmp.indexOf(";", idy + 1);
					int userId = Integer.valueOf(tmp.substring(idx + 1, idy));
					int userRank = Integer.valueOf(tmp.substring(idy + 1, idz));
					c.userSet.add(userId);
					idx = idz;
				}
				centerList.add(c);
			}

			try {
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			} finally {
				cleanup(context);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "assign");
		job.setJarByClass(Assign.class);
		
		job.setMapperClass(AssignMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("cluster/data"));
		FileOutputFormat.setOutputPath(job, new Path("cluster/assign"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
