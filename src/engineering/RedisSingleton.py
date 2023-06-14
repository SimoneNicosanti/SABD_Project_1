import redis
import jproperties as jprop


__connection = None

def getRedisConnection() -> (redis.Redis) :
    global __connection
    if (__connection == None) :
        configs = jprop.Properties()
        with open("./properties/redis.properties", 'rb') as config_file:
            configs.load(config_file)

            hostName = configs.get("redis.host").data
            portNumber = configs.get("redis.port").data
            dbNumber = configs.get("redis.db").data

            __connection = redis.Redis(
                host = hostName,
                port = portNumber,
                db = dbNumber
            )

    return __connection