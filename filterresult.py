__author__ = 'kevin'
f = open('/home/kevin/Desktop/false_test')
id_names = {}
def get_ids():
    for l in f.readlines():
        origin_id,find_id,_,match_hash = l.split()
        yield origin_id, find_id, match_hash


id_file = open('/home/kevin/Desktop/id_name')

for l in id_file.readlines():
    indexes = l.rfind(',')
    name=l[0:indexes]
    id=l[indexes+1:len(l)-1]
    id_names[int(id)] = name

outfile = open('/home/kevin/Desktop/filter_result','w')
for origin_id, find_id, match_hash in get_ids():
    outfile.write('orig_id: {0} songname: {1} \nfind_id: {2} songname:{3}\nmatch_hash_num: {4}\n '.format(origin_id, id_names[int(origin_id)], find_id, id_names[int(find_id)], match_hash))
    print('orig_id: {0} songname: {1} \nfind_id: {2} songname:{3}\nmatch_hash_num: {4}\n '.format(origin_id, id_names[int(origin_id)], find_id, id_names[int(find_id)], match_hash))

outfile.close()

