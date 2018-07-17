#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main GetRes.java
jar cf getres.jar GetRes*.class
