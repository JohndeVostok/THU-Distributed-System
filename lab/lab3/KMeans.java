import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;

public class KMeans {
	
	public static class Center {
		public int movieId;
		public Set <Integer> userSet = new HashSet();
		public Map <Integer, Float> rankMap = new HashMap();
	}

	public static class KMeansMapper extends Mapper <Object, Text, Text, Text> {
		private Map <Integer, Center> centerMap = new HashMap();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			List <Integer> centerList = new ArrayList();
			Set <Integer> userSet = new HashSet();
			Map <Integer, Integer> rankMap = new HashMap();
			int idx = str.indexOf("\t"), idy = str.indexOf("|"), idz;
			int movieId = Integer.valueOf(str.substring(0, idx));
			while (idx < idy - 1) {
				idz = str.indexOf(",", idx + 1);
				int centerId = Integer.valueOf(str.substring(idx + 1, idz));
				centerList.add(centerId);
				idx = idz;
			}
			idx = idy;
			while (idx < str.length() - 1) {
				idy = str.indexOf(",", idx + 1);
				idz = str.indexOf(";", idy + 1);
				int userId = Integer.valueOf(str.substring(idx + 1, idy));
				int userRank = Integer.valueOf(str.substring(idy + 1, idz));
				userSet.add(userId);
				rankMap.put(userId, userRank);
				idx = idz;
			}

			int cid = -1;
			Float min = 2F;
			for (int id : centerList) {
				if (!centerMap.containsKey(id)) {
					continue;
				}
				Center c = centerMap.get(id);
				Set <Integer> tmp = new HashSet();
				tmp.addAll(userSet);
				tmp.retainAll(c.userSet);
				Float t0 = 0f, t1 = 0f, t2 = 0f;
				for (int uid : tmp) {
					t0 += rankMap.get(uid) * c.rankMap.get(uid);
				}
				for (int uid : userSet) {
					t1 += rankMap.get(uid) * rankMap.get(uid);
				}
				for (int uid : c.userSet) {
					t2 += c.rankMap.get(uid) * c.rankMap.get(uid);
				}
				Float d = t0 * t0 / t1 / t2;
				d = 0.5f - d * 0.5f;
				if (d < min) {
					min = d;
					cid = id;
				}
			}

			if (cid == -1) {
				return;
			}
			Text keyInfo = new Text(String.valueOf(cid));
			Text valueInfo = new Text();
			StringBuffer buf = new StringBuffer();
			for (int k : rankMap.keySet()) {
				int v = rankMap.get(k);
				buf.append(String.valueOf(k));
				buf.append(",");
				buf.append(String.valueOf(v));
				buf.append(";");
			}
			buf.append("|");
			buf.append("1");
			valueInfo.set(buf.toString());
			context.write(keyInfo, valueInfo);
		}
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
		
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			FSDataInputStream in = hdfs.open(new Path(context.getConfiguration().get("centerPath")));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String tmp = null;
			while ((tmp = reader.readLine()) != null) {
				Center c = new Center();
				int idx = tmp.indexOf("\t");
				int movieId = Integer.valueOf(tmp.substring(0, idx));
				c.movieId = movieId;
				while (idx < tmp.length() - 1) {
					int idy = tmp.indexOf(",", idx + 1);
					int idz = tmp.indexOf(";", idy + 1);
					int userId = Integer.valueOf(tmp.substring(idx + 1, idy));
					float userRank = Float.valueOf(tmp.substring(idy + 1, idz));
					c.userSet.add(userId);
					c.rankMap.put(userId, userRank);
					idx = idz;
				}
				centerMap.put(c.movieId, c);
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
	
	public static class KMeansReducer extends Reducer <Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			Map <Integer, Integer> rankMap = new HashMap();
			int cid = Integer.valueOf(key.toString());
			int cnt = 0;
			for (Text value : values) {
				String str = value.toString();
				int idx = -1, idy = str.indexOf("|"), idw, idz;
				while (idx < idy - 1) {
					idw = str.indexOf(",", idx + 1);
					idz = str.indexOf(";", idw + 1);
					int userId = Integer.valueOf(str.substring(idx + 1, idw));
					int userRank = Integer.valueOf(str.substring(idw + 1, idz));
					if (!rankMap.containsKey(userId)) {
						rankMap.put(userId, 0);
					}
					rankMap.put(userId, rankMap.get(userId) + userRank);
					idx = idz;
				}
				idx = idy;
				cnt += Integer.valueOf(str.substring(idx + 1, str.length()));
			}
			StringBuffer buf = new StringBuffer();
			for (int k : rankMap.keySet()) {
				Float v = rankMap.get(k) / (float)cnt;
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

		String centerPath = "cluster/iter" + args[0] + "/part-r-00000";
		conf.set("centerPath", centerPath);
		Job job = Job.getInstance(conf, "kmeans");

		job.setJarByClass(KMeans.class);
		
		job.setMapperClass(KMeansMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setReducerClass(KMeansReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path("cluster/assign"));
		FileOutputFormat.setOutputPath(job, new Path("cluster/iter" + String.valueOf(Integer.valueOf(args[0]) + 1)));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
