#!/bin/sh
source ./env.sh
hdfs dfs -rm -r output_kmers
hadoop jar ./K_mers/kmers.jar KMers ecoli.fa output_kmers/
hdfs dfs -cat output_kmers/*
