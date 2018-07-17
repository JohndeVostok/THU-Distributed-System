# 分布式系统 lab2

计54 马子轩 2015012283

## 实验目标

在wiki数据使用map reduce跑page rank




## 实验过程

### 建图

由于集群资源紧张, 建图改为本地建图.

通过正则表达式匹配页面的标题和转到的链接, 并进行标准化.

同时去除掉无效链接.

```java
public static String parse(String value) throws Exception {
	String keyInfo, valueInfo;
	Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
	String[] strs = parseText(value);
	String buf = "";
	if (!validPage(strs[0])) {
		return "";
	}
	keyInfo = strs[0].replace(" ", "_");
	if (!map.containsKey(keyInfo)) {
		return "";
	}
	buf += String.valueOf(map.get(keyInfo)) + "\t1.0;";
	Matcher matcher = linkPattern.matcher(strs[1]);
	while (matcher.find()) {
		String page = matcher.group();
		page = formatPage(page);
		if (page == null || page.isEmpty()) {
			continue;
		}
		valueInfo = page;
		if (!map.containsKey(valueInfo)) {
			continue;
		}
		buf += String.valueOf(map.get(valueInfo)) + ",";
	}
	buf = buf.substring(0, buf.length() - 1) + "\n";
	return buf;
}
```



### 使用Map Reduce进行迭代求解

实际过程就是2部分.

Map部分将输入转化为

page(target) -> rank(base), links

同时添加page(base) -> |pageInfo, 用来方便下一轮迭代

```java
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
```

Reduce的时候进行计算

$PR_j=(\Sigma PRi /Link_i) * damping + 1 - damping$

```java
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
```

这个过程我迭代了10轮.数据基本稳定.

实验过程中主要问题出现在对page的了解较少.page名称中有较多的特殊字符.导致开始用map reduce做parse的时候遇到了非常大的困难. 后来调对了又改了单机. 计算过程实际上是比较容易的.



### 数据汇总

我使用了一个map函数进行这步操作.实际就是把map的内容反过来了一下.

```java
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
```



## 实验总结

1. 时间安排, 本来我是觉得这个实验应该在5个小时以内连写带跑. 结果实际上由于parse的问题, 光把这步调过了就用了那么长时间. 最终可能总时间在10小时以上.

2. 帮助内容, 在parse这件事上由于自己遇到了过大的困难, 去google了一下别人是怎么做的. 然后改进了自己的方法.

3. 我做的时候有点偷懒没用combiner. 不过我自己想了一下.好像是可以用起来的. 如果把图和pagerank中间数据分开的话. 就是在我的代码中. 把|links这个去掉. 在其他文件中进行加载. 然后map的时候调用links的信息生成page->float(rank/linknum)这个形式的中间值.这样combiner就可以进行简单的求和. 之后再在reduce阶段*0.85 + 0.15即可.

4. 在rank最高的10个网页为

|排名|编号|条目|rank|
|-|-|-|-|
|1|4188103|United States|29298.984|
|2|1250908|France|10320.461|
|3|2306711|United_Kingdom|10292.244|
|4|4305533|Germany|9342.429|
|5|3293913|World_War_II|8004.5215|
|6|3320133|Canada|7937.602|
|7|1450542|England|7618.4546|
|8|7005677|India|6610.6333|
|9|6411386|Japan|6390.814|
|10|5376569|The_New_York_Times|6123.4976|

5. 我一开始的时候由于使用map reduce进行开始的xml parse. 因此产生的中间数据都是page的名字而不是编号. 后来改成编号后. 我思考一个问题, 就是这两个速度会不会有很大的差别. 于是我先比较了文件大小. 纯数字文件大小在1g左右. 而全page名的时候文件大小在2.8g左右. 但是两者进行10轮迭代的时间是差不多的. 因此在这个问题上. 和文件的大小并没有那么直接的线性关系. 仔细思考一下. 我认为, 由于在处理数据的时候是一次处理一行. 而处理这一行的时间其实和里面内容的实际长度关系并不大. 主要还是里面包含多少信息. 而这两点两种表示方式是一致的. 因此时间差不多是可以理解的.
