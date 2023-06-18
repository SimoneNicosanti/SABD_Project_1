#!/bin/bash
docker compose up --detach --scale spark-worker=2

## Init HDFS
docker compose exec master /HDFS/init_hdfs.sh