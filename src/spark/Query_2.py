from pyspark import *
from pyspark.statcounter import StatCounter
import time


def query(rdd : RDD) -> tuple([list, float]) :
    ## @param rdd : RDD of ['TradingDate', 'TradingTime', 'ID', 'SecType', 'Last', 'TradingTimeHour']

    # partialRdd = rdd.map( # [(TradingDate, ID, TradingTimeHour), (TradingTime, Last)]
    #     lambda x : ((x[0], x[2], x[5]), (x[1], x[4]))
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
    # ).map( ## ((TradingDate, ID, Hour), (Last))
    #     lambda x : ((x[0][0], x[0][1], int(x[0][2][0:2]) + 1) , x[1][3])
    # ).map( ## ((TradingDate, ID) , (Hour, Last))
    #     lambda x : ((x[0][0], x[0][1]), (x[0][2], x[1]))
    # )

    partialRdd = rdd.map( # [(TradingDate, ID, TradingTimeHour), (TradingTime, Last, TradingTime, Last)]
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

    


    # print("Collecting Result")
    # for elem in resultRdd.collect() :
    #     print(elem)

    # return

    ## Anziché ordinare cerca il max per 5 volte e il min per 5 volte
    ## Ordinamento costa O(n^2) oppure O(n * log n)
    ## Ricerca del max e del min mi costa O(10 * n) = O(n) visto che 10 << #tuple per elemento
    ## ho 10 * n < n * log(n) ; le liste hanno lunghezza al più 500 e quindi ordinare e poi prendere le prime parti della lista è più efficient
    
    ## TODO Ricontrolla Tuple Count: fatto con il +1 !! 
    ## Se faccio il +1 sto considerando il primo prezzo che invece nelle variazioni sparisce.
    ## Sia qui che in Sql
    ## Le azioni che hanno una sola tupla totale sono filtrate nel join

    # TODO Prendere quello che ha TradingHour più piccolo?? Non è necessario in teoria ma andrebbe fatto: giustificalo con analisi del Dataset

    print("Collecting result of Second Query")
    start = time.time()
    resultRdd.collect()
    end = time.time()
    print("Execution Time >>> ", end - start)

    resultRdd = resultRdd.map(
        lambda x : (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2])
    )

    return (resultRdd, end - start)