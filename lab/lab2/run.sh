#!/bin/bash

rm -r output
set -x
hdfs dfs -rm -r -f output
hadoop jar ii.jar InvertedIndex
hdfs dfs -get output .
