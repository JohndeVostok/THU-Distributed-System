#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main PageRanker.java
jar cf ii.jar PageRanker*.class
