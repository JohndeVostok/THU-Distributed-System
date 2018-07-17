#!/bin/bash

set -x
hadoop jar kmeans.jar KMeans 0
hadoop jar kmeans.jar KMeans 1
hadoop jar kmeans.jar KMeans 2
hadoop jar kmeans.jar KMeans 3
hadoop jar kmeans.jar KMeans 4
hadoop jar kmeans.jar KMeans 5
hadoop jar kmeans.jar KMeans 6
hadoop jar kmeans.jar KMeans 7
hdfs dfs -get test/iter8 .
