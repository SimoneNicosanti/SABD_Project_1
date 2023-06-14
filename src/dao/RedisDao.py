from redis import *
from engineering import RedisSingleton
import pickle


def putResult(resultList : list, query : str) :
    redisConnection : Redis = RedisSingleton.getRedisConnection()
    for result in resultList :
        
        redisConnection.hset(
            name = query,
            key = convertToString(result[0]), 
            value = convertToString(result[1])
        )

        redisConnection.hmget()


def convertToString(tuple : tuple) :
    result = ""
    for elem in tuple :
        result = result + str(elem) + "#"
    return result