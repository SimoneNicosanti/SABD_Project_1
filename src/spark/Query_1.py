from pyspark import *
import time
from pyspark.statcounter import StatCounter

def query(rdd : RDD) -> tuple([RDD, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    resultRDD = rdd.filter(
        lambda x : x[3] == "E" and str(x[2]).endswith(".FR")
    ).map( ## ((TradingDate, TradingHour, ID), Last)
        lambda x : ( (x[0], x[5], x[2]), x[4])
    ).aggregateByKey( ## ((TradingDate, TradingHour, ID), StatsOf(count, mean, stddev, max, min))
        zeroValue = StatCounter(),
        seqFunc = StatCounter.merge,
        combFunc = StatCounter.mergeStats
    ).mapValues( ## ((TradingDate, TradingHour, ID), (min, avg, max, count))
        lambda x : (x.min(), x.mean(), x.max(), x.count())
    ) 

    
    print("Collecting result of First Query")
    start = time.time()
    resultRDD.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    resultRDD = resultRDD.map(
        lambda x : (x[0][0], x[0][1], x[0][2], x[1][0], x[1][1], x[1][2], x[1][3])
    )

    return (resultRDD, end - start)

