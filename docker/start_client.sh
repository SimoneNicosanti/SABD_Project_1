#!/bin/bash

docker build ../ -f dockerfile.client -t spark-client
cd ../
docker run -t -i -p 4040:4040 --network my_network --name=client --volume ./src:/src --volume ./Results:/Results --workdir /src spark-client 
docker exec -t -d client sh -c "chmod 777 /Results"