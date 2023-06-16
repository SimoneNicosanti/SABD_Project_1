#!/bin/bash


# Init HDFS
docker compose exec master hdfs namenode -format
docker compose exec master /usr/local/hadoop/sbin/start-dfs.sh

# Move Dataset.csv to HDFS and removal from HDFS Master
docker compose exec master hdfs dfs -put /dataset/Dataset.csv /Dataset.csv
docker compose exec master hdfs dfs -chmod 777 /
docker compose exec master hdfs dfs -mkdir /Results
docker compose exec master hdfs dfs -chmod 777 /Results