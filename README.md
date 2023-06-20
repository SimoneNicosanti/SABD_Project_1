# SABD_Project_1
This repository contains a code that permits to analyze data of Infront Financial Technology dataset about trades happened between November 8th 2021 and November 14th 2021. We answered the following queries:
* **Query_1.** For each hour, calculate the minimum, average and maximum value of the selling price (Last field) for the only stocks (SecType field with value equal to E) traded on the Paris (FR) markets. In the output
also indicate the total number of events used to calculate the statistics. Attention should be paid to events with no payload.
* **Query_2.** For each day, for each stock traded on any market, calculate the mean value and standard deviation of their sales price change calculated over a one-hour time window. After calculating the statistical indices on a daily basis, determine the ranking of the best 5 stocks that recorded the best price change during the day and the 5 worst stocks that recorded the worst price change. In the output also indicate the number total number of events used to calculate the statistics
* **Query_2.** For each day, calculate the 25th, 50th, 75th percentile of the change in the price of sale of stocks traded on individual markets. The statistic should be calculated by considering all and only the stocks belonging to each specific market: Paris (FR), Amsterdam (NL) and Frankfurt/Xetra (ETR). In the output also indicate the total number of events used to calculate the statistics.

We implemented the queries both with RDD and DataFrame using Apache Spark; the code for the queries can be found at:
* Spark. *src/spark/Query_\**
* SparkSQL. *src/spark/SqlQuery_\**

## Requirements
---
This project uses **Docker** and **Docker Compose** to instantiate HDFS, Spark and Redis

## Deployment
---
To deply this project run from */docker* directory the command:
```bash
start_all.sh <numOfSparkWorkers>
```
You can stop all containers running from */docker* directory the command:
```bash
stop_all.sh
```
After starting you can scale number of spark workers running from */docker* directory the command:
```bash
docker compose up --detach --scale spark-worker=<numOfSparkWorkers>
```

After system deployment you will be attached to bash in a client container

## Execute Query
---
You can run queries for time evaluation running:
```bash
python3 Main.py
```
This command will run all queries with both RDD and DataFrame without writing output, but saving execution times in */Results/evaluation/evaluations.csv*

You can run queries using the following command:
```bash
python3 Main.py <queryNum> <framework> <writeOutput>
```
With **queryNum** :
* 1 &rarr; Query_1
* 2 &rarr; Query_2
* 3 &rarr; Query_3
* else &rarr; All

With **framework** :
* 1 &rarr; RDD
* 2 &rarr; DataFrame
* else &rarr; Both

With **writeOutput** :
* 0 &rarr; True
* else &rarr; False

If run with **writeOutput** as **True**, query results will be written in:
* */Results/spark/Query_\** if run with RDD
* */Results/spark/SqlQuery_\** if run with DataFrame