from pyspark import *
import time
from engineering import SparkSingleton

def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    #SparkSingleton.SparkSingleton.getInstance().getSparkContext().addPyFile("/src/spark/ComputePercentiles.py")
    

    def computePercentile(valueList : list, perc : float) -> tuple :
        listLen = len(valueList)
        product = listLen * perc
        digits = product - int(product)
        result = 0

        if (digits == 0) :
            firstIndex = int(product)
            secondIndex = int(product) + 1

            firstValue = valueList[firstIndex]
            secondValue = valueList[secondIndex]

            result = (firstValue + secondValue) / 2

        else :
            index = int(product)

            result = valueList[index]
    
        return result


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
        lambda x : ( 
            (x[0][0], str(x[0][1])[str(x[0][1]).index(".") + 1 :]), 
            x[1] 
        )
    ).groupByKey(

    ).map( ## ((Date, Country), listOfVariations)
        lambda x : (x[0], sorted(list(x[1])))
    ).map(
        lambda x : (
        x[0], 
        (computePercentile(x[1], 0.25),
        computePercentile(x[1], 0.5),
        computePercentile(x[1], 0.75))
        )
    )

    print("Collecting result of Third Query")
    start = time.time()
    resultList = result.collect()
    end = time.time()

    # for result in resultList :
    #     print(result)
    #     print("\n")


    return (resultList, end - start)




