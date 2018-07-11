#!/bin/bash

set -x
hdfs dfs -rm -r wiki-num
hdfs dfs -mkdir wiki-num
hdfs dfs -put wiki-num/iter0 wiki-num/iter0
hadoop jar pr.jar PageRanker 0 1
hadoop jar pr.jar PageRanker 1 2
hadoop jar pr.jar PageRanker 2 3
hadoop jar pr.jar PageRanker 3 4
hadoop jar pr.jar PageRanker 4 5
hadoop jar pr.jar PageRanker 5 6
hadoop jar pr.jar PageRanker 6 7
hadoop jar pr.jar PageRanker 7 8
hadoop jar pr.jar PageRanker 8 9
hadoop jar pr.jar PageRanker 9 10
hdfs dfs -get wiki-num/iter10 wiki-num
