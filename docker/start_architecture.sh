#!/bin/bash
docker compose up --detach --scale spark-worker=$1

## Init HDFS
docker compose exec master /HDFS/init_hdfs.sh