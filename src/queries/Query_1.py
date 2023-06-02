from pyspark import *
import time


def query(rdd : RDD) -> tuple([RDD, float]) :

    result = rdd.filter(
        lambda x : x[1] == "E" and x[0].endswith(".FR")
    ).map(
        lambda x : ( (x[0], x[3], x[5]) , x[2] ) 
    ).aggregateByKey(
        zeroValue = (float('inf'), 0, -float('inf'), 0), # min, sum, max, count
        seqFunc = lambda accum, elem : ( 
            min(accum[0], elem), 
            accum[1] + elem, 
            max(accum[2], elem), 
            accum[3] + 1 
        ),
        combFunc = lambda accum_1 , accum_2 : (
            min(accum_1[0], accum_2[0]), 
            accum_1[1] + accum_2[1], 
            max(accum_1[2], accum_2[2]), 
            accum_1[3] + accum_2[3]
        )
    ).map(
        lambda x : (x[0], (x[1][0], x[1][1] / x[1][3], x[1][2], x[1][3]))
    )

    print("Collecting result of First Query")
    start = time.time()
    resultList = result.collect()
    end = time.time()

    print(resultList[0], end - start)


    return (result, end - start)