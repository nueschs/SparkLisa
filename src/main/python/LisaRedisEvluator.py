import redis
import numpy
numpy.seterr(all='print')
with open('../resources/node_values_4.txt', 'rb') as f:
    data = f.readlines()
count = 0
sum_ = 0.0
avg = 0
allNodesVals = []
allNodesLisa = {}

for line in data:
    nodes_values = [float(x) for x in line.strip().split(';')]
    nc = 1
    count += 4
    sum_ += sum(nodes_values)
    allNodesVals.extend(nodes_values)
    avg = sum_ / count
    stddev = numpy.std(allNodesVals)

    for node_val in nodes_values:
        val_id = '"node'+str(nc)+'_'+str((count / 4) + (count % 4))+'"'
        nc += 1
        lisa = (node_val - avg) / stddev
        print(val_id, lisa, '"'+str(lisa)+'"')
        allNodesLisa[val_id] = '"'+str(lisa)+'"'

r = redis.StrictRedis(host="localhost", port=6379, db=0)

allLisaValuesMap = r.hgetall("\"allLisaValues\"")
print('=============================================')
for k, v in allLisaValuesMap.items():
    if v != allNodesLisa[k]:
        print(k, v, allNodesLisa[k])






