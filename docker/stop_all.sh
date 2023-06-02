#!/bin/bash

docker kill client
docker rm client

docker compose down

