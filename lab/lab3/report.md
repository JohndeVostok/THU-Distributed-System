# 分布式系统 lab3

计54 马子轩 2015012283

## 实验目标

使用netflix数据, 通过map-reduce进行canopy k-means聚类.

## 实验过程

### 数据预处理

使用了一段单机代码进行数据预处理.

选取投票次数最多的100000个用户. 使用这100000个用户构造数据集. 并重新编号.

查询合适的用户

```java
public static void main(String[] args) throws Exception {
	Map map = new HashMap <Integer, Integer>();

	String path = "netflix/training_set";
	File file = new File(path);
	File[] files = file.listFiles();
	int cnt = 0;
	for (File filein : files) {
		BufferedReader reader = new BufferedReader(new FileReader(filein));
		String tmp = reader.readLine();
		while ((tmp = reader.readLine()) != null) {
			int idx = tmp.indexOf(",");
			int userId = Integer.valueOf(tmp.substring(0, idx));
			if (!map.containsKey(userId)) {
				map.put(userId, 0);
			}
			map.put(userId, (int) map.get(userId) + 1);
		}
		reader.close();
		System.out.println("File" + String.valueOf(cnt) + " finish.\n");
		cnt++;
	}
	List <Map.Entry <Integer, Integer>> list = new ArrayList <Map.Entry <Integer, Integer>> (map.entrySet());
	Collections.sort(list, new Comparator <Map.Entry <Integer, Integer>> () {
		@Override
		public int compare(Map.Entry <Integer, Integer> o1, Map.Entry <Integer, Integer> o2) {
			return o2.getValue().compareTo(o1.getValue());
		}
	});
	File fileout = new File("cluster/userlist");
	BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
	for (int i = 0; i < 100000; i++) {
		Map.Entry entry = list.get(i);
		writer.write(String.valueOf(entry.getKey()) + "\n");
	}
	writer.close();
}
```

构造数据

```java
public static void main(String[] args) throws Exception {
	Map map = new HashMap <Integer, Integer>();
	File fileuser = new File("cluster/userlist");
	BufferedReader userreader = new BufferedReader(new FileReader(fileuser));
	String tmp = null;
	for (int i = 0; i < 100000; i++) {
		tmp = userreader.readLine();
		int userId = Integer.valueOf(tmp);
		map.put(userId, i);
	}
	File fileout = new File("cluster/data/data");
	BufferedWriter writer = new BufferedWriter(new FileWriter(fileout));
	String path = "netflix/training_set";
	File file = new File(path);
	File[] files = file.listFiles();
	int cnt = 0;
	for (File filein : files) {
		System.out.println(filein.length());
		BufferedReader reader = new BufferedReader(new FileReader(filein));
		tmp = reader.readLine();
		StringBuffer str = new StringBuffer(tmp);
		while ((tmp = reader.readLine()) != null) {
			int idx = tmp.indexOf(",");
			int userId = Integer.valueOf(tmp.substring(0, idx));
			int idy = tmp.indexOf(",", idx + 1);
			int rank = Integer.valueOf(tmp.substring(idx + 1, idy));
			if (map.containsKey(userId)) {
				str.append(String.valueOf(map.get(userId)));
				str.append(",");
				str.append(String.valueOf(rank));
				str.append(";");
			}
		}
		reader.close();
		writer.write(str.toString());
		writer.newLine();
		System.out.println("File" + String.valueOf(cnt) + " finish.");
		cnt++;
	}
	writer.close();
}
```

### canopy

我使用了一轮map reduce的canopy算法. 

map阶段, 在每个mapper构造一个中心列表. 每次map的时候, 和中心列表比较. 满足强距离关系的点直接跳过. 否则加入中心列表.

```java
public static class CanopyMapper extends Mapper <Object, Text, Text, Text> {
	private List <CanopyCenter> centerList = new ArrayList();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String str = value.toString();
		Set <Integer> userSet = new HashSet();
		Map <Integer, Integer> rankMap = new HashMap();
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

		for (CanopyCenter c : centerList) {
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
			for (CanopyCenter c : centerList) {
				key.set(String.valueOf(c.movieId));
				StringBuffer buf = new StringBuffer();
				for (int userId : c.userSet) {
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
```

reduce阶段, 对map阶段的所有中心再进行一次这个操作. 将上一步的中心合并.

```java
public static class CanopyReducer extends Reducer <Text, Text, Text, Text> {
	private List <CanopyCenter> centerList = new ArrayList();

	@Override
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
		Set <Integer> userSet = new HashSet();
		Map <Integer, Integer> rankMap = new HashMap();
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

		for (CanopyCenter c : centerList) {
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
			for (int k : userSet) {
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
		for (CanopyCenter c : centerList) {
			key.set(String.valueOf(c.movieId));
			StringBuffer buf = new StringBuffer();
			for (int userId : c.userSet) {
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
```

### Assign node to canopy

先读取canopy center的列表. 然后进行map, 判断属于哪些canopy, 并进行标记.

标记成movie id -> list of canopy centers | list of (user : rank)

```java
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
```

### K-means

map阶段, map成center -> list of (user : rank) | 1

```java
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
```

reduce阶段, 对rank和count求和. 求均值.

```java
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
```

### Assign node to K-means

map阶段, 读取所有的center. 然后判断每个点属于哪个center.

## 实验总结

1. 时间安排, 因为周六爸爸过生日. 于是周五晚上回家了.  导致周日其实才开始干活, 时间很紧张. 本来预计要做10h左右, 但是由于对k-means算法的时间估计较短. 在等运行上等了很长时间. 好在最后延长了. 我估计实际使用时间可能多于20小时.
2. 帮助内容, canopy的实现上我一开始的想法其实是对的, 但是实现细节上有问题. 在经过和助教的讨论后确定把这个细节忽略掉了. 就顺利实现了. 
3. 提高聚类结果质量. 我觉得可以通过降低canopy的阈值. 增大范围. 从而使更多的数据入围. 也就能提高结果的质量了. 而为了计算速度, 不能那么做, 因此在这个地方通过canopy的调参来进行trade-off. 
4. 在做这个实验的过程中, 因为我这两天比较迷糊, 所以犯了挺多的不应该的错误, 浪费了很长时间. 比如cos距离比较反了等. 挺不应该的. 一开始, 在canopy的时候我就在纠结第一个问题. 就是说使用这种方式构造的canopy是和使用串行算法构造出来的不一样的. 因为我们定义的距离其实不是距离. 然后想了比较长的时间. 这一部分是通过和助教讨论解决的. 也就是不管他. 之后就是k-means, 一开始是parse的时候写了几个bug. 调了很久. 最大的一个bug就是. cos距离定义. 我直接求了cos值当距离用. 但是显然这是做反了. 然后发现点越迭代越少. 后来改正之后就比较顺利了.
