# -*- coding: utf-8 -*-
import cProfile
import re
import time
from flask import Flask
import MySQLdb

from flask import request
import redis
import json
from urlparse import parse_qs
from collections import defaultdict
app = Flask(__name__)
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

        r = redis.Redis()
        result_str_arr = r.smembers('h:' + hash_value)


        if result_str_arr and len(result_str_arr) > 0:
            match_hash_num = max_match_num + 1

        for str_value in result_str_arr:
            find_landmark_num = find_landmark_num + 1

            song_id, start_time_song = parse_id_starttime(str_value)

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
                    print "song id: " , s_id
                    name_printed = True
                top25=top25+1
                print("delta t: ", dt, "hashnum: ", num)

    song_name = r.get('song_id:' + str(final_id))
    print song_name
    print "find landmark num " ,find_landmark_num
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
    result = find_match(data,song_id)
    return result

@app.route('/querymysql', methods=['GET', 'POST'])
def querymysql():

    if request.method == 'POST':
        data = request.data
        return query_in_database_multisql(data)
    return ""

db=MySQLdb.connect(host='localhost', user='root', passwd='123456', db='fingerprint', charset="utf8")
def get_id_time(hash):
    cur = db.cursor()
    query_sql = 'select songid, starttime from hashes where hash=%s;' %(hash)
    cur.execute(query_sql)
    results = cur.fetchall()
    if results:
        for result in results:
            yield result

    cur.close()

def get_song_name(songid):
    cur = db.cursor()
    query_sql = 'select name from songs where id =%s' % (songid)
    cur.execute(query_sql)
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
    sql = "select * from hashes where hash in %s ;" %(hash_array_str)
    cur.execute(sql)
    while True:
        results = cur.fetchmany(1000)
        if not results:
            break
        for result in results:
            yield tuple(result)
    cur.close()

def query_in_database(d):
    max_hash_num = 0
    final_song_id = -1
    final_delta_t = -1
    result_dic = defaultdict(lambda:defaultdict(lambda:0))
    hash_time_dic = defaultdict(lambda:[])
    hash_array = get_hash_array(d,hash_time_dic)
    for hash,id,start_time in get_next_hash_id_time(hash_array):
        for start_time_in_query in hash_time_dic:
            delta_t = int(start_time) - int(start_time_in_query)
            result_dic[id][delta_t] += 1
            hnum = result_dic[id][delta_t]
            if max_hash_num  < hnum:
                final_song_id = id
                max_hash_num = hnum
                final_delta_t = delta_t

    #     start_time_in_query, hash = tuple(time_hash.split(':'))
    #     id_times = get_id_time(hash)
    #     for id,time in id_times:
    #         delta_t = int(time) - int(start_time_in_query)
    #         result_dic[id][delta_t] += 1
    #         hnum = result_dic[id][delta_t]
    #         if max_hash_num  < hnum:
    #             final_song_id = id
    #             max_hash_num = hnum
    #             final_delta_t = delta_t

    songname = get_song_name(final_song_id)
    # print "match hash num ", match_hash_num
    # print "hash num: " ,  hash_num

    print "max_hash_num", max_hash_num
    print "song name" , songname or ""

    ret =  json.dumps({
        'id':str(final_song_id)
        ,'delta_t':str(final_delta_t)
        })

    db.close()
    print ret
    return ret

def query_in_database_multisql(d):
    max_hash_num = 0
    final_song_id = -1
    final_delta_t = -1
    result_dic = defaultdict(lambda:defaultdict(lambda:0))

    for time_hash in next_time_hash(d):
         start_time_in_query, hash = tuple(time_hash.split(':'))
         id_times = get_id_time(hash)
         for id,time in id_times:
             delta_t = int(time) - int(start_time_in_query)
             result_dic[id][delta_t] += 1
             hnum = result_dic[id][delta_t]
             if max_hash_num  < hnum:
                 final_song_id = id
                 max_hash_num = hnum
                 final_delta_t = delta_t

    songname = get_song_name(final_song_id)
    # print "match hash num ", match_hash_num
    # print "hash num: " ,  hash_num

    print "max_hash_num", max_hash_num
    print "song name" , songname or ""

    ret =  json.dumps({
        'id':str(final_song_id)
        ,'delta_t':str(final_delta_t)
        })

    db.close()
    print ret
    return ret
# def query_in_database(d):
    # find_landmark_num = 0
    # match_hash_num = 0
    # hash_num = 0
    # result_dic = defaultdict(lambda:defaultdict(lambda:0))
    # final_id = -1
    # final_delta_t = -1
    # max_match_num = 0
    # db=MySQLdb.connect(host='localhost', user='root', passwd='123456', db='fingerprint', charset="utf8")
    # cur = db.cursor()
    # for h in nexthash(d):
    #     hash_num = hash_num + 1
    #     value = h.split(':')
    #     if len(value) != 2:
    #         print value
    #     start_time = int(value[0])
    #     hash_value = value[1]
    #     query_sql = 'select songid, starttime from hashes where hash=%s;' %(hash_value)
    #     cur.execute(query_sql)
    #     for song_id,start_time_in_song in cur.fetchall():
    #         delta_t = start_time_in_song - start_time
    #         if delta_t >= 0:
    #            result_dic[song_id][delta_t] += 1
    #            if result_dic[song_id][delta_t] > max_match_num:
    #                max_match_num = result_dic[song_id][delta_t]
    #                final_id = song_id
    #                final_delta_t = delta_t
    #
    # real_song_hash_match = -1
    # for (s_id, song_value) in result_dic.iteritems():
    #     for (dt,num) in song_value.iteritems():
    #         if song_id == s_id:
    #             if real_song_hash_match < num:
    #                 real_song_hash_match = num
    #
    # print "find landmark num " ,find_landmark_num
    # print "match hash num ", match_hash_num
    # print "hash num: " ,  hash_num
    # ret =  json.dumps({'id':str(final_id),'delta_t':str(final_delta_t)
    #     , 'hash_num':str(hash_num)
    #     , 'match_hash_num':str(match_hash_num)
    #     , 'max_match_hash_num':str(max_match_num)
    #     , 'song_name':""})
    #
    # print ret
    # return ret
if __name__ == '__main__':
    # app.run(host='172.18.184.41')
    f = open('/home/kevin/Desktop/post_data')
    d = f.read()

    t1 = time.time()
    cProfile.run('query_in_database_multisql(d)')
    t2 = time.time()
    print 'mysql time: ',t2 - t1
    cProfile.run('find_match(d,28)')
    t3 = time.time()
    print 'redis time: ', t3 - t2
