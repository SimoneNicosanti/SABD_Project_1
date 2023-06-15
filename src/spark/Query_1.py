from pyspark import *
import time
from pyspark.statcounter import StatCounter

def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    resultRDD = rdd.filter(
        lambda x : x[3] == "E" and str(x[2]).endswith(".FR")
    ).map( ## ((TradingDate, TradingHour, ID), Last)
        lambda x : ( (x[0], x[5], x[2]), x[4])
    ).aggregateByKey( ## ((ID, TradingDate, TradingHour), StatsOf(count, mean, stddev, max, min))
        zeroValue = StatCounter(),
        seqFunc = StatCounter.merge,
        combFunc = StatCounter.mergeStats
    ).map( ## ((ID, TradingDate, TradingHour), (min, avg, max, count))
        lambda x : (x[0], (x[1].min(), x[1].mean(), x[1].max(), x[1].count()))
    ) 

    
    print("Collecting result of First Query")
    start = time.time()
    resultList = resultRDD.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultList, end - start)

