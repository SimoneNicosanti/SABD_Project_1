from pyspark import *
from pyspark.statcounter import StatCounter
import time


def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    def findVariationList(couplesList : list) :
        valuesList = sorted(list(couplesList))

        varList = []
        for i in range(1, len(valuesList)) :
            firstElem = valuesList[i - 1]
            secondElem = valuesList[i]
            varList.append(secondElem[1] - firstElem[1])
        
        return varList

    resultRdd = rdd.map( # [(TradingDate, ID, TradingTimeHour), (TradingTime, Last, TradingTime, Last)]
        lambda x : ((x[0], x[2], x[5]), (x[1], x[4], x[1], x[4]))
    ).reduceByKey( ## ((TradingDate, ID, TradingHour), (minTime, minPrice, maxTime, maxPrice))
        lambda x, y : (
            min(x[0], y[0]),
            x[1] if x[0] < y[0] else y[1],
            max(x[2], y[2]),
            x[3] if x[2] > y[2] else y[3]
        )
    ).map( ## ((TradingDate, ID, Hour), (Last))
        lambda x : ((x[0][0], x[0][1], int(x[0][2][0:2]) + 1) , x[1][3])
    ).map( ## ((TradingDate, ID) , (Hour, Last))
        lambda x : ((x[0][0], x[0][1]), (x[0][2], x[1]))
    ).groupByKey(

    ).mapValues(
        lambda x : findVariationList(x)
    ).flatMap(
        lambda x : ((x[0], varElem) for varElem in x[1])
    ).aggregateByKey(
        zeroValue = StatCounter(),
        seqFunc = StatCounter.merge,
        combFunc = StatCounter.mergeStats
    ).map( ## ((TradingDate, ID), (Mean, StdDev, Count + 1)) --> +1 is for counting first tuple of variations
         lambda x : (x[0], (x[1].mean(), x[1].stdev(), x[1].count() + 1))
    ).map( ## (TradingDate, (Mean, StdDev, Count, ID))
        lambda x : ( x[0][0], (x[1][0], x[1][1], x[1][2], x[0][1]) ) 
    ).groupByKey( ## (TradingDate, iterableOf((Mean, StdDev, Count, ID)))

    ).mapValues( ## (TradingDate, sortedListOf((Mean, StdDev, Count, ID)))
        lambda x : sorted(list(x), reverse = True)
    ).mapValues( ## (TradingDate, listOf((Mean, StdDev, Count, ID)))
        lambda x : x[0 : 5] + x[-5 : ]
    ).flatMap( ## ((TradingDate, ID), (Mean, StdDev, Count))
        lambda x : [ ((x[0], elem[3]) , (elem[0], elem[1], elem[2])) for elem in x[1] ]
    )

    print("Collecting result of Second Query (Variant)")
    start = time.time()
    resultRdd.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    resultRdd = resultRdd.map(
        lambda x : (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2])
    )

    return (resultRdd, end - start)