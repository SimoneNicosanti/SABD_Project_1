from pyspark import *
from math import sqrt
from pyspark.statcounter import StatCounter
import time
import statistics


def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    partialRdd = rdd.map( # [(TradingDate, ID, TradingTimeHour), (TradingTime, Last)]
        lambda x : ((x[0], x[2], x[5]), (x[1], x[4]))
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
    ).map( ## ((TradingDate, ID, Hour), (Last))
        lambda x : ((x[0][0], x[0][1], int(x[0][2][0:2]) + 1) , x[1][3])
    ).map( ## ((TradingDate, ID) , (Hour, Last))
        lambda x : ((x[0][0], x[0][1]), (x[0][2], x[1]))
    )

    resultRdd = partialRdd.join( ## ((TradingDate), ((Hour_1, Last_1), (Hour_2, Last_2)))
        partialRdd
    ).filter( ## ((TradingDate), ((Hour_1, Last_1), (Hour_2, Last_2)))
        lambda x : x[1][0][0] < x[1][1][0]
    ).map( ## ((TradingDate, ID, Hour_2), (Hour_1, Last_1, Last_2))
        lambda x : ((x[0][0], x[0][1], x[1][1][0]), (x[1][0][0], x[1][0][1], x[1][1][1]))
    ).reduceByKey( ## ((TradingDate, ID, Hour_2), (maxHour, maxLast, Last_2))
        lambda x, y : (max(x[0], y[0]), x[1] if x[0] > y[0] else y[1], x[2])
    ).map( ## ((TradingDate, ID), Variation)
        lambda x : ((x[0][0], x[0][1]), x[1][2] - x[1][1])
    ).aggregateByKey( ## ((TradingDate, ID), (Stats))
        zeroValue = StatCounter(),
        seqFunc = StatCounter.merge,
        combFunc = StatCounter.mergeStats
    ).map( ## ((TradingDate, ID), (Mean, StdDev, Count))
        lambda x : (x[0], (x[1].mean(), x[1].stdev(), x[1].count()))
    ).map( ## (TradingDate, (Mean, StdDev, Count, ID))
        lambda x : ( x[0][0], (x[1][0], x[1][1], x[1][2], x[0][1]) ) 
    ).groupByKey(

    ).mapValues(
        lambda x : sorted(list(x), reverse = True)
    ).mapValues(
        lambda x : x[0 : 5] + x[-5 : ]
    ).flatMap(
        lambda x : [ ((x[0], elem[3]) , (elem[0], elem[1], elem[2])) for elem in x[1] ]
    )


    # print("Collecting Result")
    # for elem in resultRdd.collect() :
    #     print(elem)

    # return

    ## Anziché ordinare cerca il max per 5 volte e il min per 5 volte
    ## Ordinamento costa O(n^2) oppure O(n * log n)
    ## Ricerca del max e del min mi costa O(10 * n) = O(n) visto che 10 << #tuple per elemento
    ## ho 10 * n < n * log(n) ; le liste hanno lunghezza al più 500 e quindi ordinare e poi prendere le prime parti della lista è più efficient
    
    ## TODO Tuple Count

    print("Collecting result of Second Query")
    start = time.time()
    resultList = resultRdd.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultList, end - start)