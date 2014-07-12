# -*- coding: utf-8 -*-
import cProfile
import re
import time
from flask import Flask
import psycopg2
from flask import request
import redis
import json
from urlparse import parse_qs
from collections import defaultdict
from flask_uwsgi_websocket import GeventWebSocket
app = Flask(__name__)
ws = GeventWebSocket(app)
app.debug=True

def next_time_hash(d):
    for h in d[:-1].split(','):
        yield h
def parse_starttime_hash(s):
    values = s.split(':')
    assert(len(values) == 2)
    return int(values[0]), values[1]

def parse_id_starttime(s):
    values = s.split(',')
    assert(len(values) == 2)
    return str(values[0]), int(values[1])
def getRedis(host='localhost',dbnum=0):
    return redis.Redis(host=host,db=dbnum)

def find_match(d, song_id):
    real_song_id = song_id
    find_landmark_num = 0
    match_hash_num = 0
    hash_num = 0
    result_dic = defaultdict(lambda:defaultdict(lambda:0))
    final_id = -1
    final_delta_t = -1
    max_match_num = 0

    for h in next_time_hash(d):
        hash_num = hash_num + 1

        start_time, hash_value = parse_starttime_hash(h)

        r = getRedis(host='172.18.184.202')
        result_str_arr = r.smembers('h:' + hash_value)


        if result_str_arr and len(result_str_arr) > 0:
            match_hash_num = max_match_num + 1

        for str_value in result_str_arr:
            find_landmark_num = find_landmark_num + 1

            song_id, start_time_song = parse_id_starttime(str_value)
            if song_id == '1101':
                continue
            delta_t = start_time_song - start_time
            if delta_t >= 0:
                result_dic[song_id][delta_t] += 1
                if result_dic[song_id][delta_t] > max_match_num:
                    max_match_num = result_dic[song_id][delta_t]
                    final_id = song_id
                    final_delta_t = delta_t
                    break # what use??!#!@#!@#!@#@#!@#
    top25=0
    second_max = 0
    second_id = 0
    real_song_hash_match = -1
    real_song_hash_match_time = -1
    for (s_id, song_value) in result_dic.iteritems():
        name_printed=False
        for (dt,num) in song_value.iteritems():
            if int(real_song_id) == int(s_id):
                if real_song_hash_match < num:
                    real_song_hash_match = num
                    real_song_hash_match_time = dt

            if num > second_max and s_id != final_id:
                second_max = num
                second_id = s_id
            if num > max_match_num - max_match_num*0.25:
                if not name_printed:
                    if s_id == final_id:
                        print u"命中歌曲"
                    print "song id: " , s_id
                    name_printed = True
                top25=top25+1
                print("delta t: ", dt, "hashnum: ", num)
    name_redis = getRedis()
    song_name = name_redis.get('song_id:' + str(final_id))
    print song_name
    print "find landmark num " ,find_landmark_num
    print "highest match hash num",max_match_num
    print "match hash num ", match_hash_num
    print "total hash num: " ,  hash_num
    ret =  json.dumps({'id':str(final_id),'delta_t':str(final_delta_t)
        ,'real_song_hash_match':str(real_song_hash_match)
        ,'real_song_hash_match_time':str(real_song_hash_match_time)
        , 'second_max_num':str(second_max)
        , 'second_id':str(second_id)
        , 'top25_num':str(top25)
        , 'total_hash_num':str(hash_num)
        , 'match_hash_num':str(match_hash_num)
        , 'hash_num':str(max_match_num)
        , 'song_name':song_name})

    print ret
    return ret
@app.route('/query', methods=['GET', 'POST'])
def query():
    data = None
    song_id = -1
    if request.method == 'POST':
        data = request.data
        query = request.query_string
        query_dict = parse_qs(query)
        try:
            song_id = query_dict['songid'][0]
        except:
            song_id = -1
    else:
        query = request.query_string
        query_dict = parse_qs(query)
        try:
            data = query_dict['fp'][0]
        except:
            return "no fp"
    if not data or data == "":
        return json.dumps({'error':"no post data"})
    print "searching database for user input"
    now = time.time()
    result = find_match(data,song_id)
    query_time = time.time() - now
    print 'query time: ',query_time
    return result
@app.route('/xzg')
def xzg():
    return "hellow i'm xzg"

@app.route('/querymysql', methods=['GET', 'POST'])
def querymysql():

    if request.method == 'POST':
        data = request.data
        return query_in_database_multisql(data)
    return ""
@ws.route('/querywebsocket')
def query_websocket(ws):
    while True:
        data = ws.receive()
        print "from client: ", data
        ws.send("server reply~~")
        continue
        # results = query_in_database_multisql(data)
        # print "results:",results
        # ws.send(results)
def getdb():
    conn = psycopg2.connect("dbname=fingerprint user=kevin password=123456")
    return conn

db=getdb()
query_hash_num = 0
query_match_hash_num = 0
find_song_time_pair = 0


def get_id_time(hash):
    global query_hash_num
    global query_match_hash_num
    global find_song_time_pair
    global db
    query_hash_num = query_hash_num + 1
    cur = db.cursor()
    query_sql = 'select songid, starttime from musicfp where hash=%s;'
    cur.execute(query_sql, (hash,))
    results = cur.fetchall()
    if results:
        query_match_hash_num = query_match_hash_num + 1
        for result in results:
            find_song_time_pair = find_song_time_pair + 1
            yield result

    cur.close()

def get_song_name(songid):
    global db
    cur = db.cursor()
    query_sql = 'select name from songs where id =%s'
    cur.execute(query_sql, (songid,))
    results = cur.fetchone()
    if results:
        cur.close()
        return results
def get_next_hash(data):
    time_hash_pairs = next_time_hash(data)
    for pair in time_hash_pairs:
        yield tuple(pair.split(':'))

def get_hash_array(data,dic):
    arr = []
    for time,hash in get_next_hash(data):
        arr.append(hash)
        dic[hash].append(time)
    return arr


def get_next_hash_id_time(hash_array):
    cur=db.cursor()
    hash_array_str = tuple(hash_array).__str__()
    sql = "select * from musicfp where hash in %s order by rand() limit 100 ;"
    cur.execute(sql, hash_array_str)
    while True:
        results = cur.fetchmany(1000)
        if not results:
            break
        for result in results:
            yield tuple(result)
    cur.close()


def query_in_database_multisql(d):
    max_hash_num = 0
    final_song_id = -1
    final_delta_t = -1
    result_dic = defaultdict(lambda:defaultdict(lambda:0))
    second_max = 0
    second_max_id = -1
    for time_hash in next_time_hash(d):
         start_time_in_query, hash = tuple(time_hash.split(':'))
         id_times = get_id_time(hash)
         for id,time in id_times:
             delta_t = int(time) - int(start_time_in_query)
             result_dic[id][delta_t] += 1
             hnum = result_dic[id][delta_t]
             if max_hash_num  < hnum:
                 if second_max_id  != final_song_id:
                     second_max_id = final_song_id
                     second_max = max_hash_num
                 final_song_id = id
                 max_hash_num = hnum
                 final_delta_t = delta_t

    #songname = get_song_name(final_song_id)
    # print "match hash num ", match_hash_num
    print "max_hash_num", max_hash_num
    #print "song name" , songname or ""

    ret =  json.dumps({
        'id':str(final_song_id)
        ,'delta_t':str(final_delta_t)
        ,'second_max_id':second_max_id
        ,'second_max_hash':second_max
        ,'find_song_time_pair':find_song_time_pair
        ,'query_hash_num':query_hash_num
        ,'query_match_hash_num':query_match_hash_num
        ,'item_per_hash':find_song_time_pair/query_match_hash_num
        })

    db.close()
    print ret
    return ret

if __name__ == '__main__':
     app.run(host='172.18.184.41')

    # f = open('/home/kevin/Desktop/post_data')
    # d = f.read()
    #
    # t1 = time.time()
    # cProfile.run('query_in_database_multisql(d)','/home/kevin/Desktop/fp.profile' + time.ctime())
    # t2 = time.time()
    # print 'mysql time: ',t2 - t1
    # cProfile.run('find_match(d,28)')
    # t3 = time.time()
    # print 'redis time: ', t3 - t2
