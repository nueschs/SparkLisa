# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'
import math
import sys
import numpy
import random
import os


def usage():
    print
    print('Usage: python topology_creator.py number_of_nodes')
    print
    print('number_of_nodes has to be a square number\n(square of an integer)')

def create_random_topology(num_nodes_):
    if not math.sqrt(num_nodes_).is_integer():
        usage()
        sys.exit(0)

    side_length = int(math.sqrt(num_nodes_))
    matrix = create_random_connected_topology(num_nodes_, side_length)
    write_matrix_bare(matrix, num_nodes_)

def find_eligible_node(nodes_edges, num_nodes_, side_length):
    eligible_nodes = {}

    for node in nodes_edges.keys():
        is_right = node % side_length == side_length - 1
        is_bottom = num_nodes_ - node <= side_length
        is_top = node < side_length
        is_left = node % side_length == 0

        append_to_eligible_nodes(eligible_nodes, nodes_edges, is_right, node+1, node)
        append_to_eligible_nodes(eligible_nodes, nodes_edges, is_bottom, node+side_length, node)
        append_to_eligible_nodes(eligible_nodes, nodes_edges, is_top, node-side_length, node)
        append_to_eligible_nodes(eligible_nodes, nodes_edges, is_left, node-1, node)

    new_node = random.choice(eligible_nodes.keys())
    edge = random.choice(eligible_nodes[new_node])

    return edge, new_node

def append_to_eligible_nodes(eligible_nodes, nodes_edges, boolean, new_node, node):
    if not boolean and new_node not in nodes_edges:
        if new_node not in eligible_nodes:
            eligible_nodes[new_node] = []
        eligible_nodes[new_node].append(node)

def create_random_connected_topology(num_nodes_, side_length,):
    nodes_edges = {0:[]}

    while len(nodes_edges.keys()) < num_nodes_:
        node, new_node = find_eligible_node(nodes_edges, num_nodes_, side_length)
        nodes_edges[new_node] = [node]
        nodes_edges[node].append(new_node)

    matrix = [[int(x) for x in y] for y in numpy.zeros((num_nodes_, num_nodes_))]

    for y, xs in nodes_edges.items():
        for x in xs:
            matrix[y][x] = 1

    return matrix


def write_matrix_bare(matrix, num_nodes_):
    file_name_base = '../resources/topology/'
    if not os.path.isdir(file_name_base): os.makedirs(file_name_base)
    file_name = file_name_base+'topology_bare_'+str(num_nodes_)+'.txt'
    if os.path.isfile(file_name): os.remove(file_name)

    file_ = open(file_name, 'wb')

    for idx, row in enumerate(matrix):
        line = ''
        for x in row:
            line += str(x)+','
        file_.write(line.rstrip().rstrip(',')+'\n')

    file_.close()

#
# num_nodes = int(sys.argv[1])
# avg_degree = float(sys.argv[2]) if len(sys.argv) > 2 else 2.5
#
# create_random_topology(num_nodes, avg_degree)