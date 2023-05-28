#!/bin/bash
docker kill slave1 slave2 slave3 master spark client
docker rm master slave1 slave2 slave3 spark client
docker network rm hadoop_network

