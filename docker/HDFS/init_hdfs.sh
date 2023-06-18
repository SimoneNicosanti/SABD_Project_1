#!/bin/bash


# Init HDFS
hdfs namenode -format
/usr/local/hadoop/sbin/start-dfs.sh

# Move Dataset.csv to HDFS and removal from HDFS Master
#curl -o /Dataset.csv www.ce.uniroma2.it/courses/sabd2223/project/out500_combined+header.csv
hdfs dfs -put /dataset/Dataset.csv /Dataset.csv
hdfs dfs -chmod 777 /
hdfs dfs -mkdir /Results
hdfs dfs -chmod 777 /Results