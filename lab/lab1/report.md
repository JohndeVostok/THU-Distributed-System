# 分布式系统 lab1

计54 马子轩 2015012283

## 实验目标

1. 在莎士比亚全集上进行词频统计. 判断哪些单词出现频率很高, 是噪声.
2. 建立简单的倒排列表, 包括到文件名.
3. 对单词进行划分. 格式转换.
4. 建立完整倒排列表, 包括到每个term在文件中的位置.
5. 实现一个完整的搜索器. 搜索指定term并返回在每个文档中的位置.



## 实验过程

### 使用Map Reduce构造倒排列表

这个过程分为三部分, Map Combine Reduce.

Map部分将输入转化为

Term:Filename -> Position

```java
public static class InvMapper extends Mapper <Object, Text, Text, Text> {
	private FileSplit split;
	private Text keyInfo = new Text();
	private Text valueInfo = new Text();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		split = (FileSplit) context.getInputSplit();
		String filename = split.getPath().getName();
		String str = value.toString();
		Pattern pattern = Pattern.compile("[^a-zA-Z]");
		Matcher matcher = pattern.matcher(str + ".");
		LongWritable tmp = (LongWritable) key;
		int base = (int) tmp.get();
		int lastOff = 0, curOff = 0;
		while (matcher.find()) {
			curOff = matcher.start();
			if (curOff - lastOff > 0) {
				String word = str.substring(lastOff, curOff).toLowerCase();
				int add = base + lastOff;
				keyInfo.set(word + ":" + filename);
				valueInfo.set(String.valueOf(add));
				context.write(keyInfo, valueInfo);
			}
			lastOff = curOff + 1;
		}
	}
}
```

Combine将其转化为

Term -> list of Filename:Positions

```java
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
```

Reduce将list进行合并

```java
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
```

即可完成倒排列表的构造, 其中Position的获取给我带来了比较大的麻烦.

实际Hadoop的Map包括三部分: File Split, Record Read, Map

而我在前两个部分一开始有所混淆. 实际上, 在运行过程中. 先根据文件大小对文件进行partition, 也就是file split.将每个文件划分成合适的大小. 然后把每个split交给mapper. 再使用record read读取每条记录. 默认一行一条. record read返回的key -> value对实际是这一行在文件中的offset和这一行的完整内容. 因此, 实际在map中直接用key加上行中offset就可以找到这个term在文件中的位置了.

而之前花了很多时间实现自己的file split和record read. 花了较多时间.

### 实现搜索器

实际就是从已经实现的倒排列表中读出term对应的内容. 并翻译成人能看的东西.

这一部分较为简单, 不多加以赘述.

```python
import os
import math

if __name__ == "__main__":
	rootDir = "shakespeare"
	dirs = os.listdir(rootDir)
	data = {}
	for filename in dirs:
		data[filename] = ["", [], [0]]
		tmpDir = os.path.join(rootDir, filename)
		with open(tmpDir, "r") as f:
			data[filename][0] = f.read()
		with open(tmpDir, "r") as f:
			data[filename][1] = f.readlines()
		tmp = 0
		for line in data[filename][1]:
			tmp += len(line)
			data[filename][2].append(tmp)

	with open("res.txt", "r") as f:
		lines = f.readlines()

	text = {}
	count = {}

	for line in lines:
		cnt = 0
		tmp = line.split("\t")
		term = tmp[0]
		res = ""
		tmp = tmp[1].split(";")
		for tmpfile in tmp:
			tmpp = tmpfile.split(":")
			filename = tmpp[0]
			res += "Filename: " + filename + "\n"
			tmppp = tmpp[1].split(",")
			for offstr in tmppp:
				offset = int(offstr)
				l = 0; r = len(data[filename][2])
				while l + 1 < r:
					mid = math.floor((l + r) / 2)
					if (data[filename][2][mid] <= offset):
						l = mid
					else:
						r = mid
				res += "line " + str(l) + ":" + data[filename][1][l]
				cnt += 1
		text[term] = res
		count[term] = cnt

	print("Initialized.")
	while True:
		term = input("Term: ")
		if not term in text:
			print("Term not existed.")
			continue
		if count[term] > 1000:
			print("Term is noise.")
			continue
		print(text[term])
```



## 实验总结

1. 时间安排, 本来觉得这个实验应该用3个小时就能搞定. 而实际上由于在offset上花费了很多的时间最终实现的时间可能需要7小时左右. 真的符合软工的*2+10%啊.
2. 帮助内容, 和大作业队友讨论过, 但是他们好像也不会. 然后我只能去查文档. 最后还是靠文档搞定的.
3. 文档位置. 大概这个题是要看真没真做实验. 文档存在HDFS的/data/shakespeare. 
4. 关于噪声. 我为了方便取了阈值1000, 次数多于1000的我就当成噪声了. 这些term包括各种介词副词什么的. 还有各种人称代词. 
5. 实验给我带来的最大的surprise就是. 我没想到hadoop会那么慢. 其实周五肯定能搞定. 但是断电了. 周六白天又疯狂排队. 最后只好半夜的时候趁没人用赶紧调对了. 没想到还被群里吐槽了. 速度慢这件事我考虑跟HDFS的读写有很大关系. 但是现在我还说不好, 还需要再调研一下. 
