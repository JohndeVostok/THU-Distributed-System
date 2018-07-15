#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main Canopy.java
jar cf canopy.jar Canopy*.class
