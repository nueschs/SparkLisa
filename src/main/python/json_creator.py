import json
import math
import random
max_edge_length = 60
svg_side_length = 1200
offset = 20

def read_topology(topology_file):
    topology = {}

    with open(topology_file, 'r') as f:
        data = f.readlines()

    topology['nodes'] = []
    topology['edges'] = []

    for i in range(0, len(data)):
        topology['nodes'].append('node'+str(i))
        edges = [int(x) for x in data[i].split(',')]
        for j in range(0, len(edges)):
            if edges[j] == 1 and j > i:
                topology['edges'].append((i,j))

    return topology


def jsonify_edges(edges):
    edges_json = []
    value = 1
    for edge in edges:
        source, target = edge
        edges_json.append({'source':source, 'target':target, 'value':value})
    return edges_json


def jsonify_nodes(nodes):
    nodes_json = []
    side_length = math.sqrt(len(nodes))
    edge_length = svg_side_length / side_length
    edge_length = edge_length if edge_length < max_edge_length else max_edge_length

    for i in range(0, len(nodes)):
        line = i / int(side_length)
        pos = i % int(side_length)
        node_id = nodes[i]
        nodes_json.append({'sensorId':node_id, 'fixed':True, 'group':1, 'x':int(pos*edge_length)+offset, 'y':int(line*edge_length)+offset})

    return nodes_json


def write_topology_json(topology_file, json_file):
    topology = read_topology(topology_file)
    topology_json = {'nodes': jsonify_nodes(topology['nodes']), 'links': jsonify_edges(topology['edges'])}
    with open(json_file, 'w') as json_:
        json.dump(topology_json, json_, indent=2)