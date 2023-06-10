from pyspark import *
import time
import statistics

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
    ).map( ## ((Date, ID), (variation, count))
        lambda x : (
            x[0], 
            (x[1][1] - x[1][3], 1 if x[1][0] == x[1][2] else 2)
        )
    ).map( ## ((Date, Country), (variation, count))
        lambda x : ( 
            (x[0][0], str(x[0][1])[str(x[0][1]).index(".") + 1 :]), 
            x[1] 
        )
    ).aggregateByKey(
        zeroValue = ([], 0) ,
        seqFunc = lambda accum, x : (accum[0] + [x[0]], accum[1] + x[1]),
        combFunc = lambda accum_1, accum_2 : (accum_1[0] + accum_2[0], accum_1[1] + accum_2[1])
    ).map( ## ((Date, Country), (listOfVariations, count))
        lambda x : (x[0], (sorted(list(x[1][0])), x[1][1]))
    ).map(
        lambda x : (x[0], (statistics.quantiles(x[1][0], n = 100), x[1][1]))
    ).map(
        lambda x : (
            x[0],
            (
                x[1][0][25],
                x[1][0][50],
                x[1][0][75],
                x[1][1]
            )
        )
    )

    ## TODO CONTROLLA CHE SIA GIUSTA LA MODIFICA CON LIBRERIA statistics
    
    # .map(
    #     lambda x : (
    #     x[0], 
    #     (computePercentile(x[1][0], 0.25),
    #     computePercentile(x[1][0], 0.5),
    #     computePercentile(x[1][0], 0.75),
    #     x[1][1])
    #     )
    # )

    print("Collecting result of Third Query")
    start = time.time()
    resultList = result.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)


    return (resultList, end - start)




