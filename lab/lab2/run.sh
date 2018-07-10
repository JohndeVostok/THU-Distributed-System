#!/bin/bash

rm -r /data/wiki-tmp
set -x
hdfs dfs -rm -r -f /data/wiki-tmp
hadoop jar ii.jar PageRanker
hdfs dfs -get /data/wiki-tmp
