__author__ = 'kevin'
import redis
f = open('/home/kevin/Desktop/id_name')
r = redis.Redis()
for l in f:
    [song_name,song_id] = l.split(',')
    song_id = song_id.strip()
    song_name = song_name.strip()
    r.set('song_id:' + str(song_id), song_name)

print