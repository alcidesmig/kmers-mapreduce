#!/bin/sh
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export PATH=$PATH:/usr/local/hadoop/bin/
alias compile='/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main'
