# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'
import math
import sys
import numpy
import random
import os


def usage():
    print
    print('Usage: python TopologyCreator.py number_of_nodes [avg_degree]')
    print
    print('number_of_nodes has to be a square number\n(square of an integer)')
    print('avg_degree represent the average number\nof connections per node (default 2.5)')


def create_random_topology(num_nodes_, avg_degree_):
    if not math.sqrt(num_nodes_).is_integer():
        usage()
        sys.exit(0)

    side_length = int(math.sqrt(num_nodes_))
    p = avg_degree_ / 4.0
    matrix = create_sparse_matrix_right_half(num_nodes_, side_length, p)
    matrix = mirror_matrix(matrix, num_nodes_, side_length)
    write_matrix_bare(matrix, num_nodes_, avg_degree_)
    print(sum(sum(x) for x in matrix))


def create_sparse_matrix_right_half(num_nodes_, side_length, p):
    matrix = [[int(x) for x in y] for y in numpy.zeros((num_nodes_, num_nodes_))]
    for i in range(0, num_nodes_):
        is_right = i % side_length == side_length - 1
        is_bottom = num_nodes_ - i <= side_length

        if not is_right: matrix[i][i + 1] = int(random.random() <= p)
        if not is_bottom: matrix[i][i + side_length] = int(random.random() <= p)

    return matrix


def mirror_matrix(matrix, num_nodes_, side_length):
    for i in range(0, num_nodes_):
        is_top = i < side_length
        is_left = i % side_length == 0

        if not is_left: matrix[i][i-1] = matrix[i-1][i]
        if not is_top: matrix[i][i-side_length] = matrix[i-side_length][i]

    return matrix


def write_matrix(matrix, num_nodes_, avg_degree_):
    file_name_base = '../resources/topology/'
    if not os.path.isdir(file_name_base): os.makedirs(file_name_base)
    file_name = file_name_base+'topology_'+str(num_nodes_)+'_'+str(avg_degree_)+'.txt'
    if os.path.isfile(file_name): os.remove(file_name)

    file_ = open(file_name, 'wb')

    header_line = ''
    for idx, _ in enumerate(matrix):
        header_line += 'node'+str(idx)+',\t'
    file_.write(header_line.rstrip().rstrip(',')+'\n')

    for idx, row in enumerate(matrix):
        line = 'node'+str(idx)+',\t'
        for x in row:
            line += str(x)+',\t'
        file_.write(line.rstrip().rstrip(',')+'\n')

    file_.close()


def write_matrix_bare(matrix, num_nodes_, avg_degree_):
    file_name_base = '../resources/topology/'
    if not os.path.isdir(file_name_base): os.makedirs(file_name_base)
    file_name = file_name_base+'topology_bare_'+str(num_nodes_)+'_'+str(avg_degree_)+'.txt'
    if os.path.isfile(file_name): os.remove(file_name)

    file_ = open(file_name, 'wb')

    for idx, row in enumerate(matrix):
        line = ''
        for x in row:
            line += str(x)+','
        file_.write(line.rstrip().rstrip(',')+'\n')

    file_.close()


num_nodes = int(sys.argv[1])
avg_degree = float(sys.argv[2]) if len(sys.argv) > 2 else 2.5

create_random_topology(num_nodes, avg_degree)