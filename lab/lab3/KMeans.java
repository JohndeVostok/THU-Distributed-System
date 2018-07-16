import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

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

public class KMeans {
	
	public static class Center {
		public int movieId;
		public Map rankMap <Integer, Integer> = new HashMap();
	}

	static {
		Map centerMap <Center> = new HashMap();
	}

	public static class KMeansMapper extends Mapper <Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			List <Center> centerList = new ArrayList();
			Map rankMap <Integer, Integer> = new HashMap();
			int idx = str.indexOf(":"), idy = str.indexOf("|"), idz;
			int movieId = Integer.valueOf(str.substring(0, idx));
			while (idx < idy - 1) {
				idz = str.indexIf(",", idx);
				int centerId = Integer.valueOf(str.substring(idx + 1, idz));
				centerList.add(centerId);
				idx = idz;
			}
			while (idx < str.length() - 1) {
				idy = str.indexOf(",", idx);
				idz = str.indexOf(";", idy);
				int userId = Integer.valueOf(str.substring(idx + 1, idy));
				int userRank = Integer.valueOf(str.substring(idy + 1, idz));
				rankMap.put(userId, userRank);
				idx = idz;
			}

			for (int id : centerList) {
				//TODO
				//get cos dist
			}

			Text keyInfo = new Text(String.valueOf(cid));
			Text valueInfo = new Text();
			StringBuffer buf = new StringBuffer();
			for (Map.Entry <Integer, Float> entry : rankMap) {
				buf.append(String.valueOf(entry.getKey()));
				buf.append(",");
				buf.append(String.valueOf(entry.getValue()));
				buf.append(";");
			}
			buf.append("|");
			buf.append("1");
		}
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			try {
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			} finally {
				cleanup(context);
			}
		}
	}
	
	public static class KMeansReducer extends Reducer <Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			Map rankMap = new HashMap <Integer, Float>();
			int cid = Integer.valueOf(key.toString());
			int cnt = 0;
			for (Text value : values) {
				String str = value.toString();
				int idx = -1, idy = str.indexOf("|"), idw, idz;
				while (idx < idy - 1) {
					idw = str.indexOf(",", idx + 1);
					idz = str.indexOf(";", idw + 1);
					int userId = Integer.valueOf(str.substring(idx + 1, idw));
					Float userRank = Float.valueOf(str.substring(idw + 1, idz));
					if (!rankMap.containsKey(userId)) {
						rankMap.put(userId, 0);
					}
					rankMap.put(userId, userMap.get(userId) + userRank);
					idx = idz;
				}
				cnt += Integer.valueOf(str.substring(idx + 1, str.length()));
			}
			StringBuffer buf = new StringBuffer();
			for (int k : rankMap.getKeys()) {
				Float v = rankMap.get(k) / cnt;
				buf.append(String.valueOf(k));
				buf.append(",");
				buf.append(String.valueOf(v));
				buf.append(";");
			}
			Text res = new Text(buf.toString());
			context.write(key, res);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "kmeans");
		job.setJarByClass(KMeans.class);
		
		job.setMapperClass(KMeansMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setCombinerClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("cluster/assign"));
		FileOutputFormat.setOutputPath(job, new Path("cluster/kmeans"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
