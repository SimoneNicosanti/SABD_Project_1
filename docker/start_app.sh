#!/bin/bash

docker kill client
docker rm client

docker build ../ -f dockerfile.client -t spark-client
cd ../
docker run -t -i --network hadoop_network --name=client --volume ./src:/src --workdir /src --publish 4040:4040  spark-client 
docker exec -t client sh -c "cd /src | python3 ./Main.py"
docker exec -t client sh -c "python3 ./Main.py"