__author__ = 'kevin'
import redis
import operator
import os
f = open('/home/kevin/Desktop/redis_script')
count = 0;
#while True:
s = ''
while count < 70:
    s += f.readline()
    if len(s) < 10:
        break;
    count += 1
os.system('echo ' + s + ' | redis-cli --pipe')
print s




