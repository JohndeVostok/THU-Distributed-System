#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main InvertedIndex.java
jar cf ii.jar InvertedIndex*.class
