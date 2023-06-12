from pyspark import *
from math import sqrt
from pyspark.statcounter import StatCounter
import time
import statistics


def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    def findValues(list : list) -> list :
        list.sort()
        minCountList = [False] * len(list)
        maxCountList = [False] * len(list)

        pricesList = []

        for i in range(0, len(list)) :
            elem = list[i]
            if (elem[1] == elem[0] + ":00.0000") :
                pricesList.append(elem[2])
                minCountList[i] = True
            else :
                j = i - 1
                if (j >= 0) :
                    prev = list[j]
                    pricesList.append(prev[4])
                    maxCountList[j] = True
        
        lastElem = list[len(list) - 1]
        pricesList.append(lastElem[4])
        maxCountList[len(list) - 1] = True

        usedCount = sum(minCountList) + sum(maxCountList)

        return (pricesList, usedCount)
    
    def findVariations(valuesList : list) -> list :
        variationList = []
        if (len(valuesList) == 1) :
            # Never changed price
            return [0]
        for i in range(1, len(valuesList)) :
            second = valuesList[i]
            first = valuesList[i - 1]
            variation = second - first
            variationList.append(variation)
        
        return variationList
    

    resultRdd = rdd.map( # [(TradingDate, ID, TradingTimeHour), (TradingTime, Last)]
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
    ).map( ## ((TradingDate, ID), (tradingHour, minTime, minPrice, maxTime, maxPrice))
        lambda x : ((x[0][0], x[0][1]), (x[0][2], x[1][0], x[1][1], x[1][2], x[1][3]))
    ).groupByKey( ## ((TradingDate, ID), listOf((tradingHour, minTime, minPrice, maxTime, maxPrice)))

    ).map( ## ((TradingDate, ID), (listOfValues, usedNumber))
        lambda x : (x[0], findValues(list(x[1])))
    ).map( ## ((TradingDate, ID), (listOfVariations, usedNumber))
        lambda x : (x[0], (findVariations(x[1][0]), x[1][1]))
    ).map( ## ((TradingDate, ID), (mean, stdDev, usedNumber))
        lambda x : (x[0], (statistics.mean(x[1][0]), 0 if len(x[1][0]) == 1 else statistics.stdev(x[1][0]), x[1][1]))
    ).map(
        lambda x : ((x[0][0]), (x[1][0], x[1][1], x[1][2], x[0][1]))
    ).groupByKey(

    ).map(
        lambda x : (x[0], sorted(list(x[1]), reverse = True))
    ).map(
        lambda x : (x[0], x[1][0 : 5] + x[1][-5 : ])
    ).flatMap(
        lambda x : [((x[0], elem[3]), (elem[0], elem[1], elem[2])) for elem in x[1]]
    )
    
    # .sortBy(
    #     lambda x : (x[0][0], x[1][0], x[1][1])
    # ).zipWithIndex(

    # )
    

    # for elem in resultRdd.collect() :
    #     print(elem)
    #     print("\n")

    # return


    print("Collecting result of Second Query")
    start = time.time()
    resultList = resultRdd.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    return (resultList, end - start)