__author__ = 'kevin'
import redis
import sys
import re

def get_name_id(str):
    pattern = re.compile(r'(.+),(\d*)')
    match = pattern.match(str)
    if match:
        groups = match.groups()
        assert(len(groups) == 2)
        name,id= match.group(1,2)
        return name, id
    else:
        raise Exception("xzg","invalid value")

dbnum = 0
if len(sys.argv) > 1:
    dbnum = sys.argv[1]
r = redis.Redis(db=dbnum)
print "song name db num:",dbnum
song_num = 0
f = open('/home/kevin/Desktop/id_name')
for l in f:
    song_num = song_num+1
    song_name,song_id = get_name_id(l)
    song_id = song_id.strip()
    song_name = song_name.strip()
    r.set('song_id:' + str(song_id), song_name)

print "added song num: ",song_num