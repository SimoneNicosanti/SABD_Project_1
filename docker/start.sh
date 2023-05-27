#!/bin/bash

# Created HDFS network
docker network create --driver bridge hadoop_network

# HDFS Master
docker build ./ -f dockerfile.hdfs_master -t hdfs-master

# HDFS Workers
docker run -t -i -p 9864:9864 -d --network=hadoop_network --name=slave1 matnar/hadoop
docker run -t -i -p 9863:9864 -d --network=hadoop_network --name=slave2 matnar/hadoop
docker run -t -i -p 9862:9864 -d --network=hadoop_network --name=slave3 matnar/hadoop

# HDFS Master
docker run -t -i -p 9870:9870 -p 54310:54310 -d --network=hadoop_network --name=master hdfs-master


# Init HDFS
docker exec -t master sh -c "hdfs namenode -format"
docker exec -t master sh -c "/usr/local/hadoop/sbin/start-dfs.sh"

# Move Dataset.csv to HDFS and removal from HDFS Master
docker exec -t master sh -c "hdfs dfs -put /Dataset.csv /Dataset.csv"
docker exec -t master sh -c "rm /Dataset.csv"

# Spark Container
docker run -t -i --network=hadoop_network --name=spark apache/spark /opt/spark/bin/pyspark