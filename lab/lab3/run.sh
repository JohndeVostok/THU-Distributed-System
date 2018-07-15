#!/bin/bash

set -x

rm -r canopy

hdfs dfs -rm -r cluster/canopy
hadoop jar canopy.jar Canopy
hdfs dfs -get cluster/canopy .
