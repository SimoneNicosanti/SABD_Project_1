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
    ).map( 
        lambda x : (x[0], (x[1].min(), x[1].mean(), x[1].max(), x[1].count()))
    ) 
    
    # .reduceByKey(
    #     lambda accum, x : (
    #         min(accum[0], x[0]),
    #         accum[1] + x[1],
    #         max(accum[2], x[2]),
    #         accum[3] + x[3]
    #     )
    # ).map(
    #     lambda x : (x[0], (x[1][0], x[1][1] / x[1][3], x[1][2], x[1][3]))
    # )


    
    # .map(
    #     lambda x : ( (x[0], x[3], x[5]) , x[2] ) 
    # ).aggregateByKey(
    #     zeroValue = (float('inf'), 0, -float('inf'), 0), # min, sum, max, count
    #     seqFunc = lambda accum, elem : ( 
    #         min(accum[0], elem), 
    #         accum[1] + elem, 
    #         max(accum[2], elem), 
    #         accum[3] + 1 
    #     ),
    #     combFunc = lambda accum_1 , accum_2 : (
    #         min(accum_1[0], accum_2[0]), 
    #         accum_1[1] + accum_2[1], 
    #         max(accum_1[2], accum_2[2]), 
    #         accum_1[3] + accum_2[3]
    #     )
    # ).map(
    #     lambda x : (x[0], (x[1][0], x[1][1] / x[1][3], x[1][2], x[1][3]))
    # )

    
    print("Collecting result of First Query")
    start = time.time()
    resultList = resultRDD.collect()
    end = time.time()

    print(resultList[0])

    return (resultList, end - start)
