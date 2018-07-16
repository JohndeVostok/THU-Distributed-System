#!/bin/bash

set -x

rm -r assign

hdfs dfs -rm -r cluster/assign
hadoop jar assign.jar Assign
hdfs dfs -get cluster/assign .
