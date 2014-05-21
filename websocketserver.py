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

find_landmark_num = 0
match_hash_num = 0
query_hash_num = 0
final_id = -1
final_delta_t = -1
max_match_num = 0

@app.route('/test')
def test():
    return "hello xzg"

@ws.route('/wstest')
def wstest(ws):
    count = 0
    while True:
        ws.receive()
        count = count + 1
        ws.send(str(count))

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
    global result_dic,find_landmark_num, match_hash_num,query_hash_num
    global final_id, final_delta_t, max_match_num,hash_record
    real_song_id = song_id

    r = redis.Redis()
    for h in next_time_hash(d):
        # deal with repeated hash
        if hash_record.has_key(h):
            continue;
        hash_record[h] = True;

        query_hash_num = query_hash_num + 1

        start_time, hash_value = parse_starttime_hash(h)

        result_str_arr = r.smembers('h:' + hash_value)

        if result_str_arr and len(result_str_arr) > 0:
            match_hash_num = match_hash_num + 1

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
                    #TODO: recall what's this means
                    #break # what use??!#!@#!@#!@#@#!@#
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
                    # print "song id: " , s_id
                    name_printed = True
                top25=top25+1
                # print("delta t: ", dt, "hashnum: ", num)

    song_name = r.get('song_id:' + str(final_id))
    second_song_name = r.get('song_id:' + str(second_id))

    print "Match song name:" ,song_name
    print "Match song name:" ,second_song_name

    print "find landmark num " ,find_landmark_num
    print "match hash num ", match_hash_num
    print "total hash num: " ,  query_hash_num
    ret = {'id':str(final_id),'delta_t':str(final_delta_t)
        ,'real_song_hash_match':str(real_song_hash_match)
        ,'real_song_hash_match_time':str(real_song_hash_match_time)
        , 'second_max_num':str(second_max)
        , 'second_id':str(second_id)
        , 'top25_num':str(top25)
        , 'query_hash_num':str(query_hash_num)
        , 'query_match_hash_num':str(match_hash_num)
        , 'max_hash_num':str(max_match_num)
        , 'find_song_time_pair':str(find_landmark_num)
        , 'song_name':song_name}

    return ret

@ws.route('/queryws')
def query_websocket(ws):

    global result_dic,find_landmark_num, match_hash_num,query_hash_num
    global final_id, final_delta_t, max_match_num,hash_record

    result_dic = defaultdict(lambda:defaultdict(lambda:0))
    find_landmark_num = 0
    match_hash_num = 0
    query_hash_num = 0
    final_id = -1
    final_delta_t = -1
    max_match_num = 0
    hash_record = {}
    while True:
        data = ws.receive()
        print "from client: ", data

        #deal with empty msg send auto by websocket
        if not data:
            continue
        #deal with ping msg
        if data[0:5] == 'ping':
            ws.send(time.time())
            continue

        query_start_time = time.time()
        ret_dict = find_match(data, -1)
        print "query_time:",(time.time() - query_start_time)

        if int(ret_dict['max_hash_num']) > 60:
            ret_dict['status'] = 'success'
        else:
            ret_dict['status'] = 'continue'

        ret = json.dumps(ret_dict)
        ws.send(ret)
        print ret

        if ret_dict['status'] == 'success':
            break

query_hash_num = 0
query_match_hash_num = 0
find_song_time_pair = 0

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

if __name__ == '__main__':
     app.run(host='172.18.184.41',port=9260,gevent=100)

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
