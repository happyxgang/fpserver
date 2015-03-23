__author__ = 'kevin'
from Queue import PriorityQueue
from Queue import Full
import redis
origin_id = 6
final_id = 33
def getRedis(host='localhost',dbnum=0):
    return redis.Redis(host=host,db=dbnum)
name_redis = getRedis()
song_name = name_redis.get('song_id:' + str(final_id))
origin_song_name = name_redis.get('song_id:' + str(origin_id))
print "origin name:", origin_song_name
print "matched name:", song_name
