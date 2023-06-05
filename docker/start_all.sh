#!/bin/bash

docker compose up --detach

# Init HDFS
docker compose exec master hdfs namenode -format
docker compose exec master /usr/local/hadoop/sbin/start-dfs.sh

# Move Dataset.csv to HDFS and removal from HDFS Master
docker compose exec master hdfs dfs -put /dataset/Dataset.csv /Dataset.csv
docker compose exec master hdfs dfs -chmod 777 /
docker compose exec master hdfs dfs -mkdir /Results
docker compose exec master hdfs dfs -chmod 777 /Results

docker build ../ -f dockerfile.client -t spark-client
cd ../
docker run -t -i -p 4040:4040 --network my_network --name=client --volume ./src:/src --volume ./Results:/Results --workdir /src spark-client 
docker exec -t -d client sh -c "chmod 777 /Results"