# 分布式系统

陈康 chenkang@tsinghua.edu.cn



## 介绍

实践: 分布式系统(6.824), OS

理论: 分布式算法

Babara Liscov, Leslie Lamport

Google File System

Map Reduce

Page Rank

Spark

Dryad

DryadLINQ



## 大作业

1. 写文件系统(不推荐)
2. N-body(模拟)
3. Search Engine
4. ML, Clustering
5. Block chain(不要在国内发币, 不要说是谁教的)



## GFS: Distributed File System

### 需求分析

运行在5000个node. 1Gbps网络上的文件系统

*文件系统: 将路径翻译为硬件的地址

分布式文件系统的Global namespace通常是user space

Filename -> Address(node ID+Disk offset or Local file)

### 实现

Master->Chunk Server

Fault tolerance: Replica

Consistent

Performance: Load Balance

Chunk server: Storage(random, 根据剩余空间访问), Access(Migration)

### Performance

Memory: 30GB/s ~10ns

Disk: 100MB/s, <1MB/s ~10ms

Metadata放入内存(FT)提高性能

使用cache

### Fault Tolerance

Chunk Server坏了,master启动re-replication, 通过并行提高性能

Master的问题

dump

10GB / 100MB/s = 100s

dump期间能不能修改

能修改的话, 会出现不一致的情况.

不能修改, 也不能2分钟什么都不干, Copy on Write.

使用log, 存在恢复速度过慢的问题. 通过snapshot完成

恢复的时候先load snapshot, 再replay log.

master, shadow

master坏了用shadow, shadow坏了赶紧修.

master->shadow->shadow'->shadow''这种方式是不可取的

性能比较差. 真实情况只有一个shadow.shadow会将自己内容备份到chunk server上. 真的出现两个机器一起死掉的情况, 通过这些数据进行恢复.

Split Brain(consensus): master觉得shadow死了, shadow觉得master死了.

分布式一致性: Paxos, Paxos made simple, Raft, ZooKeeper.

### Consistent

Replicated State Machine

state0 -> op -> state1

op: determinant

Paxos: 投票每一个操作.

定时间: master(不好, 操作太多, master压力太大). chunk server(单个也不好, 挂了怎么办)

全局顺序来定一个全序关系.

master A1(Primary) A2(Secondary) A3(Secondary)

Primary来定序

Lease: 在一段时间里面, 这个机器是Primary, 用这台机器定顺序. 时间过去之后根据情况再说.

写入动作

1. client向master要块的编号
2. master回复
3. 写给数据块
4. 向primary要顺序
5. primary定顺序,发给secondary.
6. secondary回复
7. primary回复client

任意一个错了, 就重新来一遍. 重写次数太多说明集群太差了.

分布式: safely(坏的事情永远不会发生), liveness(好的事情最终会发生)

#### 作业1

Posix File System

最后一次写进数据等于读出的数据. GFS不保证, 为什么? 如何保证. 有什么优缺点.

这里写表示写成功的情况.

#### 文件系统

open/read/write/create

delete (dir, file)

删除时候在master删除metadata, 之后等待garbage collection.



## Map Reduce

Map Reduce

1. 好用
2. Auto-scale
3. Auto-FT

### 函数

```
Map(Key, Value) {
    Emit(Key, Value)    
}

Reduce(Key, Value_list) {
    Output() -> HDFS    
}
```

### Word count

```
T1: today is good
T2: the weather is good
T3: good good study
```

```
<good, 4> <is, 2> <today, 1> <the, 1> <weather, 1> <study, 1>
```

简单做法,hashmap

Map Reduce

```
Map(Key, Value) {
	for each w in Value
	Emit(w, 1)
}

Reduce(Key, Value_list) {
	c := sum(Value_list)
	Output(Key, c)
}
```

Map -> Shuffle(排序) -> Reduce

### Hadoop编程

#### main

设置输入输出的目录格式, 运行

#### map

执行chunk次map

#### reduce

按key执行

实际执行过程中, 在同一点map的数据先合并.

### 实验1 Inverted Index

莎士比亚全集

输出,Inverted index

```
Map(docID, text) {
	for each w in text
	Emit(w, docID)
}

Reduce(w, docID_list) {
	merge()
	Output(w, docID_list)
}
```

### 实验2 Page Rank

Reverse Link

```
Map(URL, Page) {
	out_link_list = parse(Page)
	Emit(out_link_list[i], URL)
}

Reduce(Key, Value_list) {
	Output(Key, Value_list)
}
```

Page Rank

$PR_i = \Sigma\frac{PR_j}{C_j}\times(1-d)+d$

```
Map(URL, Page) {
	for each outURL
		Emit(outURL, PR/C(URL))
}

Reduce(outURL, PRs) {
	PR = sum(PRs) * (1 - d) + d
	Output(outURL, PR)
}
```

40GB wikipedia Page Rank





