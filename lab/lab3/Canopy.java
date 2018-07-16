import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

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

public class Canopy {
	
	public static class CanopyCenter {
		public int movieId;
		public Set userSet = new HashSet <Integer>();
		public Map rankMap = new HashMap <Integer, Integer>();
	}

	public static class CanopyMapper extends Mapper <Object, Text, Text, Text> {
		private List centerList = new ArrayList <CanopyCenter>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			Set userSet = new HashSet <Integer>();
			Map rankMap = new HashMap <Integer, Integer>();
			int idx = str.indexOf(":");
			int movieId = Integer.valueOf(str.substring(0, idx));
			while (idx < str.length() - 1) {
				int idy = str.indexOf(",", idx);
				int idz = str.indexOf(";", idy);
				int userId = Integer.valueOf(str.substring(idx + 1, idy));
				int userRank = Integer.valueOf(str.substring(idy + 1, idz));
				userSet.add(userId);
				rankMap.put(userId, userRank);
				idx = idz;
			}

			boolean flag = true;

			for (Object co : centerList) {
				CanopyCenter c = (CanopyCenter) co;
				Set tmp = new HashSet();
				tmp.addAll(c.userSet);
				tmp.retainAll(userSet);
				System.out.println(tmp.size());
				if (tmp.size() >= 8) {
					flag = false;
				}
			}

			if (flag) {
				CanopyCenter c = new CanopyCenter();
				c.movieId = movieId;
				for (Object uo : userSet) {
					int k = (Integer) uo;
					int v = rankMap.get(k);
					c.userSet.add(k);
					c.rankMap.put(k, v);
				}
				centerList.add(c);
			}
		}

		@Override
		public void run(Context context) throws IOException, InterruptedException {
			Text key = new Text();
			Text value = new Text();
			setup(context);
			try {
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			} finally {
				for (Object co : centerList) {
					CanopyCenter c = (CanopyCenter) co;
					key.set(String.valueOf(c.movieId));
					StringBuffer buf = new StringBuffer();
					for (Object uo : c.userSet) {
						int userId = (Integer) uo;
						int userRank = c.rankMap.get(userId);
						buf.append(String.valueOf(userId));
						buf.append(",");
						buf.append(String.valueOf(userRank));
						buf.append(";");
					}
					value.set(buf.toString());
					context.write(key, value);
				}
				cleanup(context);
			}
		}
	}
	
	public static class CanopyReducer extends Reducer <Text, Text, Text, Text> {
		private List centerList = new ArrayList <CanopyCenter>();

		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			Set userSet = new HashSet <Integer>();
			Map rankMap = new HashMap <Integer>();
			int movieId = Integer.valueOf(key.toString());
			for (Text value : values) {
				String str = value.toString();
				int idx = -1, idy, idz;
				while (idx < str.length() - 1) {
					idy = str.indexOf(",", idx + 1);
					idz = str.indexOf(";", idy + 1);
					int userId = Integer.valueOf(str.substring(idx + 1, idy));
					int userRank = Integer.valueOf(str.substring(idy + 1, idz));
					userSet.add(userId);
					rankMap.put(userId, userRank);
					idx = idz;
				}
				break;
			}

			boolean flag = true;

			for (Object co : centerList) {
				CanopyCenter c = (CanopyCenter) co;
				Set tmp = new HashSet();
				tmp.addAll(c.userSet);
				tmp.retainAll(userSet);
				if (tmp.size() >= 8) {
					flag = false;
				}
			}

			if (flag) {
				CanopyCenter c = new CanopyCenter();
				c.movieId = movieId;
				for (Object uo : userSet) {
					int k = (Integer) uo;
					int v = rankMap.get(k);
					c.userSet.add(k);
					c.rankMap.put(k, v);
				}
				centerList.add(c);
			}
		}
	
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			Text key = new Text();
			Text value = new Text();
			setup(context);
			while (context.nextKey()) {
				reduce(context.getCurrentKey(), context.getValues(), context);
			}
			for (Object co : centerList) {
				CanopyCenter c = (CanopyCenter) co;
				key.set(String.valueOf(c.movieId));
				StringBuffer buf = new StringBuffer();
				for (Object uo : c.userSet) {
					int userId = (Integer) uo;
					int userRank = c.rankMap.get(userId);
					buf.append(String.valueOf(userId));
					buf.append(",");
					buf.append(String.valueOf(userRank));
					buf.append(";");
				}
				value.set(buf.toString());
				context.write(key, value);
			}
			cleanup(context);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "canopy");
		job.setJarByClass(Canopy.class);
		
		job.setMapperClass(CanopyMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setReducerClass(CanopyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("cluster/data"));
		FileOutputFormat.setOutputPath(job, new Path("cluster/canopy"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
