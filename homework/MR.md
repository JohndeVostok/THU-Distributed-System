# Map Reduce

计54 马子轩

仔细阅读MapReduce这篇论文，进行思考，并回答下面的问题。

#### 题目1

请仔细阅读下面这段关于MapReduce的伪代码（使用Hadoop MapReduce）。这段代码最终将会输出某一个大型文档集合中的出现次数最多的三个单词。

```
void Map(String DocID, Text content) {
	HashMap<word, count> wordmap=new HashMap(……);
	for each word in content{
		if word not in wordmap
			wordmap.put(word,1);
		else
			wordmap.get(word).count++;
	}
	Emit(“donemap”,wordmap);
}

void Reduce(String key, Iterator<HashMap> maps) {
	HashMap<word, count> allwords = new HashMap(……);
	List<word, count> wordlist = new List(……);

	for map in maps{
		for each (word, count) in map
			if word not in allwords
				allwords.put(word,count)
			else
				allwords.get(word)+=count;
	}

	for each (word, count) in allwords
		wordlist.add(word,count);
	sort(wordlist) on count;
	Emit(wordlist.get(0));
	Emit(wordlist.get(1));
	Emit(wordlist.get(2));
}
```

小华同学运行上述的代码，发现部分reducers会出现OutOfMemoryException的错误。请结合代码分析其原因(不要指出语法错误)。

针对上述错误，你有什么修改方案？请简要说明你的修改方案。

这段程序本质上是在reduce中把所有文档的hashmap进行了合并, 相当于在同一个机器上把所有工作全做了, 显然这种做法是不合适的. 等于把reduce阶段的并行全部放弃掉了.

修改方案, 我认为直接使用wordcount, 再对结果进行统计即可. 能保证map和reduce阶段都能有效利用集群的资源. 同时一个节点的任务量也不会那么大.

#### 题目2

在论文中提到了，为了能够提高系统执行的速度，会采用投机执行的办法（speculative）。投机执行为何能够提高系统执行的速度？投机执行是否总是有效的，如果是否的话举出投机执行失效的场景？

我做这个题的时候都怀疑人生了, 我他妈根本不知道还有这个机制, 看了几遍之后我明白了, 这个根本就不是map reduce提供的机制, 而是hadoop实现的时候实现的机制. 他的原理是这样, 在输入规模足够大的时候, 他会假设资源分配不均匀, 于是他会在资源比较富裕的节点上重复提交其他节点上的map task或reduce task. 这时候先完成的把后完成的kill掉. 从而使负载均衡, 提高整体效率. 假设每个map task和reduce task是完全一样的. 比如说word count中每个文档词数一样, 每个词长度都一样. 所有机器性能都一致, 这样speculative机制只会降低整体运行效率.

#### 题目3

一个MapReduce程序运行在100个节点上。每个节点可以同时运行4个任务，或者是4个Map任务，或者是4个Reduce任务。假设程序中需要运行的工作是40,000个map任务以及5,000个reduce任务。假设有一个节点坏掉了，那么最多需要重启多少个map任务？最多需要重启多少个reduce任务？为什么。

如果master节点挂了, 那么重启多少个map或reduce任务取决于上一个checkpoint的位置, 即上一个checkpoint后执行的map task数和reduce task数. 如果挂掉的是worker节点, 那么只需要在其他节点上重新执行当前节点正在运行的task即可. 即最多4个map task或reduce task.