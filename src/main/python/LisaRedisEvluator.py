import redis
import numpy
numpy.seterr(all='print')

nodeMap = {
    'node1': ['node2'],
    'node2': ['node1', 'node3'],
    'node3': ['node2', 'node4'],
    'node4': ['node3']
}

# with open('../resources/node_values_4.txt', 'rb') as f:
#     data = f.readlines()
# count = 0
# sum_ = 0.0
# avg = 0
# allNodesVals = []
# allNodesLisa = {}
#
# for line in data:
#     nodes_values = [float(x) for x in line.strip().split(';')]
#     nc = 1
#     count += 4
#     sum_ += sum(nodes_values)
#     allNodesVals.extend(nodes_values)
#     avg = sum_ / count
#     stddev = numpy.std(allNodesVals)
#
#     for node_val in nodes_values:
#         val_id = '"node'+str(nc)+'_'+str((count / 4) + (count % 4))+'"'
#         nc += 1
#         lisa = (node_val - avg) / stddev
#         print(val_id, lisa, '"'+str(lisa)+'"')
#         allNodesLisa[val_id] = '"'+str(lisa)+'"'

r = redis.StrictRedis(host="localhost", port=6379, db=0)

redisAllValuesMap = r.hgetall("\"allValues\"")
allValuesMap = dict()
finalLisaValues = r.hgetall("\"finalLisaValues\"")
finalLisaValues = {k: round(float(v.strip('"')), 13) for k, v in finalLisaValues.items()}
print('=============================================')

for k, v in redisAllValuesMap.items():
    nodeID, count, _ = k.split('_')
    nodeID = nodeID.strip('"')

    if not str(count) in allValuesMap:
        allValuesMap[str(count)] = dict()

    allValuesMap[str(count)][nodeID] = float(v.strip('"'))

calculatedLisaValues = dict()

for count, v in allValuesMap.items():
    calculatedLisaValues[str(count)] = dict()
    mean = numpy.average(v.values())
    stdev = numpy.std(v.values())

    for l, w in v.items():
        neighbourVals = [v[x] for x in nodeMap[l]]
        neighbourLisa = numpy.average([((x-mean)/stdev) for x in neighbourVals])
        lisaVal = (w-mean)/stdev
        calculatedLisaValues[str(count)][l] = round(lisaVal*neighbourLisa, 13)

goodCount = 0
badCount = 0

for k, v in finalLisaValues.items():
    nodeID, count, _ = k.strip('"').split('_')

    if v == calculatedLisaValues[count][nodeID]:
        goodCount += 1
    else:
        print(v, calculatedLisaValues[count][nodeID], count, nodeID)
        badCount += 1

print(goodCount, badCount)









# for k, v in allLisaValuesMap.items():
#     if v != allNodesLisa[k]:
#         print(k, v, allNodesLisa[k])






