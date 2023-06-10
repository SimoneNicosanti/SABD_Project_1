from pyspark import *
from math import sqrt
from pyspark.statcounter import StatCounter
import time
import statistics



def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    # resultRdd : RDD = rdd.map( ## ((TradingDate, ID, TradingHour) ; (TradingTime, Last))
    #     lambda x : ( (x[0], x[2], x[5]) , (x[1], x[4]) ) 
    # ).aggregateByKey( ## ((TradingDate, ID, TradingHour), (minTime, minPrice, maxTime, maxPrice))
    #     zeroValue = ("AA:AA:AA.AAAA", 0, "00:00:00.0000", 0),
    #     seqFunc = lambda accum, elem : (
    #         min(accum[0], elem[0]),
    #         accum[1] if accum[0] < elem[0] else elem[1],
    #         max(accum[2], elem[0]),
    #         elem[1] if accum[2] < elem[0] else accum[3],
    #     ),
    #     combFunc = lambda accum_1, accum_2 : (
    #         min(accum_1[0], accum_2[0]) ,
    #         accum_1[1] if accum_1[0] < accum_2[0] else accum_2[1] ,
    #         max(accum_1[2], accum_2[2]) ,
    #         accum_1[3] if accum_1[2] > accum_2[2] else accum_2[3] ,
    #     )
    # ).map( ## ((TradingDate, ID, TradingHour), (variation, count))
    #     lambda x : (x[0], (x[1][1] - x[1][3], 1 if x[1][0] == x[1][2] else 2))
    # ).map( ## ((TradingDate, ID), (variation, count))
    #     lambda x : ((x[0][0], x[0][1]), (x[1][0], x[1][1]))
    # ).aggregateByKey( ## ((TradingDate, ID), (sum, squareSum, count))
    #     zeroValue = (0, 0, 0) ,
    #     seqFunc = lambda accum, x : (
    #         accum[0] + x[0],
    #         accum[1] + x[0] ** 2,
    #         accum[2] + 1
    #     ) ,
    #     combFunc = lambda accum_1, accum_2 : (
    #         accum_1[0] + accum_2[0] ,
    #         accum_1[1] + accum_2[1] ,
    #         accum_1[2] + accum_2[2]
    #     )
    # ).map( ## ((TradingDate, ID), (avg, squareSumDivided, count))
    #     lambda x : (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2], x[1][2]))
    # ).map( ## ((TradingDate, ID), (avg, stdDev, count))
    #     lambda x : (x[0], (x[1][0], sqrt(x[1][1] - x[1][0] ** 2), x[1][2]))
    # )

    def findFirstValues(list : list) -> list :
        list.sort()
        resultList = []
        minCountList = [False] * len(list)
        maxCountList = [False] * len(list)

        secondList = []
        for i in range(0, len(list) - 1) :
            firstHour = int(list[i][0][0:2])
            secondHour = int(list[i + 1][0][0:2])
            for j in range(firstHour, secondHour) :
                secondList.append()

        for i in range(0, len(list)) :
            elem = list[i]
            if (elem[1] == elem[0] + ":00.0000") :
                resultList.append((elem[0], elem[2]))
                minCountList[i] = True
            else :
                j = i - 1
                if (j >= 0) :
                    prev = list[j]
                    resultList.append((elem[0], prev[4]))
                    maxCountList[j] = True

        usedCount = sum(minCountList) + sum(maxCountList)

        return (resultList, usedCount)
    
    def findVariations(list : list) -> list :
        resultList = []
        if (len(list) == 0 or len(list) == 1) :
            return [0]
        for i in range(1, len(list)) :
            elem = list[i]
            prev = list[i - 1]
            variation = elem[1] - prev[1]
            resultList.append(variation)
        
        return resultList

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
    ).groupByKey(

    ).map(
        lambda x : (x[0], sorted(list(x[1])))
    )

    for elem in resultRdd.collect() :
        print(elem)
        print("\n")

    return


    print("Collecting result of Second Query")
    startTime = time.time()
    resultList = resultRdd.collect()
    endTime = time.time()
    print("Execution Time >>> ", endTime - startTime)

    return (resultList, endTime - startTime)