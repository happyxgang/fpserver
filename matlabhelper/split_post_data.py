__author__ = 'kevin'
from collections import defaultdict
f = open('/home/kevin/Desktop/post_data')
post_data_str = f.read()

plot_data_dict = defaultdict(lambda:0)

post_data = post_data_str.split(',')
post_data.pop()
for d in post_data:
    t = int(d.split(':')[0])
    plot_data_dict[t] += 1
x = []
y = []
for (time, counts) in sorted(plot_data_dict.items(),key=lambda x:x[0]):
    x.append(time)
    y.append(counts)
f = open('/home/kevin/Desktop/plot_data_hist','w+')
for v in x:
    f.write(str(v) + ',')
f.write('\n')
for v in y:
    f.write(str(v) + ',')
