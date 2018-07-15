#!/bin/bash

set -x
hdfs dfs -rm -r cluster
hdfs dfs -mkdir cluster
hdfs dfs -put cluster/data cluster
hadoop jar canopy.jar Canopy
hdfs dfs -get cluster/canopy .
