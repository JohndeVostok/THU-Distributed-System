import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
	public static class PageLinkMapper extends Mapper <Object, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private static final Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = parseText(value);
			if (!validPage(strs[0])) {
				return;
			}

			keyInfo.set(strs[0].replace(" ", "_"));
			Matcher matcher = linkPattern.matcher(strs[1]);
			while (matcher.find()) {
				String page = matcher.group();
				page = formatPage(page);
				if (page == null || page.isEmpty()) {
					continue;
				}
				valueInfo.set(page);
				context.write(keyInfo, valueInfo);
			}
		}
		
		private String[] parseText(Text value) throws CharacterCodingException {
			String[] strs = new String[2];
			int l = value.find("<title>");
			int r = value.find("</title>", l);
			if (l == -1 || r == -1) {
				return new String[] {"", ""};
			}
			l += 7;
			strs[0] = Text.decode(value.getBytes(), l, r - l);
			l = value.find("<text");
			l = value.find(">", l) + 1;
			r = value.find("</text>", l) - l;
			if (l == -1 || r == -1) {
				return new String[] {"", ""};
			}
			l += 1;
			strs[1] = Text.decode(value.getBytes(), l, r - l);
			return strs;
		}

		public boolean validPage(String str) {
			return !str.contains(":");
		}
		
		private String formatPage(String str) {
			int l = 1;
			if (str.startsWith("[[")); {
				l = 2;
			}

			if (str.length() < l + 2 || str.length() > 100) {
				return null;
			}

			char ch = str.charAt(l);
			if (ch == '#' || ch == '.' || ch == '\'' || ch == '-' || ch == '{') {
				return null;
			}

			if (str.contains(":") || str.contains(",") || str.contains("&")) {
				return null;
			}
			int r = str.indexOf("]");
			int t = str.indexOf("|");
			if (t > 0) {
				r = t;
			}
			t = str.indexOf("#");
			if (t > 0) {
				r = t;
			}
			str = str.substring(l, r);
			str = str.replace(" ", "_");
			return str;
		}

	}

	public static class LinkReducer extends Reducer <Text, Text, Text, Text> {
		private Text res = new Text();
		
		@Override
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer buf = new StringBuffer("1.0\t");
			for (Text value : values) {
				buf.append(value.toString() + ",");
			}
			res.set(buf.substring(0, buf.length() - 1));
			context.write(key, res);
		}
	}

	public static boolean parsePage() throws Exception {
		Configuration conf = new Configuration();
	
		Job parseJob = Job.getInstance(conf, "parse page");
		parseJob.setJarByClass(PageRanker.class);
		
		parseJob.setMapperClass(PageLinkMapper.class);
		parseJob.setMapOutputKeyClass(Text.class);
		parseJob.setMapOutputValueClass(Text.class);
		parseJob.setReducerClass(LinkReducer.class);
		parseJob.setOutputKeyClass(Text.class);
		parseJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(parseJob, new Path("/data/wiki-test"));
		FileOutputFormat.setOutputPath(parseJob, new Path("/data/wiki-tmp/iter0"));
		return parseJob.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		boolean flag = parsePage();
	}
}
