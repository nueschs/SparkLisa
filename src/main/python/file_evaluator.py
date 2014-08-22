# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'

import sys
import numpy
import math

values_file = sys.argv[1]
results_file = sys.argv[2]


nodeMap = {
    'node1': ['node2'],
    'node2': ['node1', 'node6'],
    'node3': ['node4'],
    'node4': ['node3', 'node8'],
    'node5': ['node6'],
    'node6': ['node2', 'node5', 'node7', 'node10'],
    'node7': ['node6', 'node8'],
    'node8': ['node4', 'node7'],
    'node9': ['node13'],
    'node10': ['node6', 'node11'],
    'node11': ['node10', 'node12', 'node15'],
    'node12': ['node11', 'node16'],
    'node13': ['node9', 'node14'],
    'node14': ['node13', 'node15'],
    'node15': ['node11', 'node14'],
    'node16': ['node12'],
}

with open(values_file, 'rb') as file:
    values = file.readlines()

with open(results_file, 'rb') as file:
    results = file.readlines()

value_map = dict()
for line in values:
    node_id, value = line.replace('(', '').replace(')', '').split(',')
    value = float(value.strip('\n'))

    if not node_id in value_map:
        value_map[node_id] = []

    value_map[node_id].append(value)

result_map = dict()
for line in results:
    node_id, value = line.replace('(', '').replace(')', '').split(',')
    value = round(float(value.strip('\n')), 13)

    if not node_id in result_map:
        result_map[node_id] = []

    result_map[node_id].append(value)


sum_ = sum([sum(b) for _, b in value_map.items()])
count = sum([len(b) for _, b in value_map.items()])
mean = sum_/count
stdDev = numpy.std([b for _, b in value_map.items()])

# lisa_map = {a: (b-mean)/stdDev for a, b in value_map.items()}
lisa_map = dict()

for k, v in value_map.items():
    lisa_map[k] = []
    for val in v:
        lisa_map[k].append((val-mean)/stdDev)

calculated_map = dict()
for k, v in lisa_map.items():
    neighbours = numpy.average([lisa_map[x] for x in nodeMap[k]])
    calculated_map[k] = round(neighbours*v[0], 13)

good_count = 0
bad_count = 0
for k, v in calculated_map.items():
    if v == result_map[k][0]:
        good_count += 1
    else:
        bad_count += 1
print(good_count, bad_count)



