#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main PageRanker.java
jar cf pr.jar PageRanker*.class

hadoop com.sun.tools.javac.Main PageFormatter.java
jar cf pf.jar PageFormatter*.class
