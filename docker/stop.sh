#!/bin/bash
docker kill slave1 slave2 slave3 master spark
docker rm master slave1 slave2 slave3 spark
docker network rm hadoop_network

