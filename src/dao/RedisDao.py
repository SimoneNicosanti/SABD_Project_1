from redis import *
from engineering import RedisSingleton
import json


def putResult(resultList : list, query : str) :
    redisConnection : Redis = RedisSingleton.getRedisConnection()

    redisTimeSeries = redisConnection.ts()

    for result in resultList :
        key = result[0][0]
        timestamp = str(result[0][1]) + "#" + result[0][2]
        value = result[1][1]
        redisTimeSeries.add(key, timestamp, value)
