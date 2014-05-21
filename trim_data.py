__author__ = 'kevin'
import csv
from collections import defaultdict
from math import ceil
import cPickle
import random
MAX_ENTRY=200
def default_zero():
    return 0

def default_array():
    return ['']*MAX_ENTRY

def nexthash():
    reader = csv.reader(file('/home/kevin/Desktop/csv_script'))
    for line in reader:
            yield line

hash_counts = defaultdict(default_zero)

hash_table = defaultdict(default_array)
for line in nexthash():
    hash,songid,time = tuple(line)
    hash_item_num = hash_counts[hash] + 1
    pos = 0
    if hash_item_num <= MAX_ENTRY:
        pos = hash_item_num
    else:
        pos = ceil(hash_item_num*random.random())
    if pos < MAX_ENTRY:
        hash_table[hash][int(pos)] = str(songid) + ',' + str(time)
    hash_counts[hash] = hash_item_num

counts_f = open('/home/kevin/hash_counts.dump','wb')
cPickle.dump(hash_counts,counts_f)
hash_table_f = open('/home/kevin/hash_table.dump','wb')
cPickle.dump(hash_table,hash_table_f)
