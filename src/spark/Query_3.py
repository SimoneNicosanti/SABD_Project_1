from pyspark import *
import time
from engineering import SparkSingleton
from spark import ComputePercentiles

def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    SparkSingleton.SparkSingleton.getInstance().getSparkContext().addPyFile("/src/spark/ComputePercentiles.py")


    result = rdd.map( ## ((Date, ID), (Time, Last, Time, Last))
        lambda x : ( (x[0], x[2]), (x[1], x[4], x[1], x[4]) )
    ).reduceByKey( ## ((Date, ID) , (minTime, minLast, maxTime, maxLast))
        lambda accum, x : (
            min(accum[0], x[0]),
            accum[1] if accum[0] < x[0] else x[1],
            max(accum[2], x[2]) ,
            accum[3] if accum[2] > x[2] else x[3]
        )
    ).map( ## ((Date, ID), variation)
        lambda x : (x[0], x[1][1] - x[1][3])
    ).map( ## ((Date, Country), variation)
        lambda x : ( (x[0][0], str(x[0][1])[str(x[0][1]).index(".") + 1 :]), x[1] )
    ).groupByKey(

    ).map( ## ((Date, Country), listOfVariations)
        lambda x : (x[0], sorted(list(x[1])))
    ).map(
        lambda x : ComputePercentiles.computePercentiles(x)
    )

    print("Collecting result of Third Query")
    start = time.time()
    resultList = result.collect()
    end = time.time()

    print(resultList[0])


    return (resultList, end - start)




