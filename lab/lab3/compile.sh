#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main KMeans.java
jar cf kmeans.jar KMeans*.class
