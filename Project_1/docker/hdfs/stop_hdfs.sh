#!/bin/bash
docker kill slave1 slave2 slave3 master
docker rm master slave1 slave2 slave3
docker network rm hadoop_network
