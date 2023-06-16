from redis import *
from engineering import RedisSingleton
from pyspark.sql import *
import json


def putResult(resultDataFrame : DataFrame, query : str) :
    redisConnection : Redis = RedisSingleton.getRedisConnection()

    redisJson = redisConnection.json()

    jsonResult = resultDataFrame.toJSON().collect()
    jsonArray = json.dumps(jsonResult)

    redisConnection.set(query, jsonArray)
