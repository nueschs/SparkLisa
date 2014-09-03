# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'

import numpy
import os

def read_values_list(values_folder):
    node_value_files = list()
    for dirpath, dirnames, filenames in os.walk(values_folder):
        for f in filenames:
            if f.endswith('.txt'):
                node_value_files.append(os.path.join(dirpath, f))

    node_values = list()
    for f in node_value_files:
        with open(f, 'rb') as fl:
            node_values.extend(fl.readlines())

    node_value_map = dict()
    for line in node_values:
        node_id = line.split('_')[0]
        count = line.split(';')[0].split('_')[1]
        value = float(line.split(';')[1])
        if not count in node_value_map:
            node_value_map[count] = dict()
        node_value_map[count][node_id] = value

    return node_value_map


def create_node_map(topology_file):
    node_map = dict()
    with open(topology_file, 'r') as f:
        data = f.readlines()

    for i in range(0, len(data)):
        connections = [int(x) for x in data[i].split(',')]
        named_connections = ['node'+str(k+1) for k,x in enumerate(connections) if x == 1]
        node_map['node'+str(i+1)] = named_connections

    return node_map


def calculate_expected_results(node_values, node_map):
    expected_results = dict()

    stdevs_per_count = {cnt : numpy.std(x.values()) for cnt, x in node_values.items()}
    means_per_count = {cnt : numpy.average(x.values()) for cnt, x in node_values.items()}
    lisa_values = {cnt : {nodeId : (val-means_per_count[cnt])/stdevs_per_count[cnt] for nodeId,val in y.items()} for cnt, y in node_values.items()}

    for cnt, y in lisa_values.items():
        expected_results[cnt] = dict()
        for node_id, lisa_val in y.items():
            neighbours = node_map[node_id]
            neighbour_val = 0
            for neighbour in neighbours:
                neighbour_val += lisa_values[cnt][neighbour]/len(neighbours)
            expected_results[cnt][node_id] = round(lisa_val*neighbour_val, 13)

    return expected_results


def parse_results(results_folder):
    results_lines = list()

    for dirpath, dirnames, filenames in os.walk(results_folder):
        for f in filenames:
            if f.startswith('part-'):
                with open(os.path.join(dirpath, f), 'rb') as fl:
                    results_lines.extend(fl.readlines())

    spark_results = dict()
    for line in results_lines:
        cnt, nodeid, lisa_val = line.replace('(','').replace(')','').split(',')

        if not cnt in spark_results:
            spark_results[cnt] = dict()
        spark_results[cnt][nodeid] = round(float(lisa_val), 13)

    return spark_results


def verify_results(values_folder, results_folder, topology_file):
    node_values = read_values_list(values_folder)
    node_map = create_node_map(topology_file)
    expected_results = calculate_expected_results(node_values, node_map)
    spark_results = parse_results(results_folder)

    bad_count = 0
    for cnt, x in spark_results.items():
        for node_id, lisa_val in x.items():
            expected = expected_results[cnt][node_id]
            if not expected == lisa_val:
                bad_count += 1
                print('\t '+cnt+'; '+node_id+'; '+str(expected)+'; '+str(lisa_val))
    print(bad_count)




