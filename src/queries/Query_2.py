from pyspark import *
from math import sqrt
from pyspark.statcounter import StatCounter
import time



def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of (ID, SecType, Last, TradingDate, TradingTime, TradingHour)

    resultRdd : RDD = rdd.map( ## ((TradingDate, ID, TradingHour) ; (TradingTime, Last))
        lambda x : ( (x[3], x[0], x[5]) , (x[4], x[2]) ) 
    ).aggregateByKey( ## ((TradingDate, ID, TradingHour), (minTime, minPrice, maxTime, maxPrice))
        zeroValue = ("AA:AA:AA.AAAA", 0, "00:00:00.0000", 0),
        seqFunc = lambda accum, elem : (
            min(accum[0], elem[0]),
            accum[1] if accum[0] < elem[0] else elem[1],
            max(accum[2], elem[0]),
            elem[1] if accum[2] < elem[0] else accum[3],
        ),
        combFunc = lambda accum_1, accum_2 : (
            min(accum_1[0], accum_2[0]) ,
            accum_1[1] if accum_1[0] < accum_2[0] else accum_2[1] ,
            max(accum_1[2], accum_2[2]) ,
            accum_1[3] if accum_1[2] > accum_2[2] else accum_2[3] ,
        )
    ).map( ## ((TradingDate, ID, TradingHour), (variation, count))
        lambda x : (x[0], (x[1][1] - x[1][3], 1 if x[1][0] == x[1][2] else 2))
    ).map( ## ((TradingDate, ID), (variation, count))
        lambda x : ((x[0][0], x[0][1]), (x[1][0], x[1][1]))
    ).aggregateByKey( ## ((TradingDate, ID), (sum, squareSum, count))
        zeroValue = (0, 0, 0) ,
        seqFunc = lambda accum, x : (
            accum[0] + x[0],
            accum[1] + x[0] ** 2,
            accum[2] + 1
        ) ,
        combFunc = lambda accum_1, accum_2 : (
            accum_1[0] + accum_2[0] ,
            accum_1[1] + accum_2[1] ,
            accum_1[2] + accum_2[2]
        )
    ).map( ## ((TradingDate, ID), (avg, squareSumDivided, count))
        lambda x : (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2], x[1][2]))
    ).map( ## ((TradingDate, ID), (avg, stdDev, count))
        lambda x : (x[0], (x[1][0], sqrt(x[1][1] - x[1][0] ** 2), x[1][2]))
    )

    print("Computing Result for Query 2")
    startTime = time.time()
    resultList = resultRdd.collect()
    print(resultList[0])
    endTime = time.time()

    return (resultList, endTime - startTime)



    
    

def getFirstAndLastTimeInfo(a : tuple, b : tuple) -> tuple :
    firstTime = min(a[0], b[0])
    lastTime = max(a[0], b[0])

    firstPrice = a[1] if a[0] < b[0] else b[1]
    lastPrice = b[1] if a[0] < b[0] else a[1]

    return (firstTime, firstPrice, lastTime, lastPrice)