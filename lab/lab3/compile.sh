#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main Assign.java
jar cf assign.jar Assign*.class
