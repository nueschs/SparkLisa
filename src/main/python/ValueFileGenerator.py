# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'
import sys
import os
from random import gauss


def create_single_file(num_nodes_, num_values_):
    file_name = '../resources/node_values_' + num_nodes_ + '_' + num_values_ + '_%.txt'

    count = 0

    while os.path.isfile(file_name.replace('%', str(count))):
        count += 1

    file_name = file_name.replace('%', str(count))
    file_ = open(file_name, 'wb')

    for i in range(0, int(num_values_)):
        line = ''
        for j in range(0, int(num_nodes_)):
            line += str(gauss(0.0, 1.0)) + ';'
        file_.write(line + '\n')
    file_.close()


def create_file_per_node(num_nodes_, num_values_):
    base_file_name = '../resources/node_values/' + num_nodes_ + '/' + num_values_ + '/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)
    for i in range(0, int(num_nodes_)):
        file_ = open(base_file_name + str(i) + '.txt', 'wb')
        for j in range(0, int(num_values_)):
            file_.write(str(gauss(0.0, 1.0)) + ';\n')
        file_.close()


def one_line_node_id_value(num_nodes_, num_values_):
    base_file_name = '../resources/node_values/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)

    file_name = base_file_name + num_nodes_ + '_' + num_values_ + '_%.txt'
    count = 0
    while os.path.isfile(file_name.replace('%', str(count))):
        count += 1
    file_name = file_name.replace('%', str(count))
    file_ = open(file_name, 'wb')
    for i in range(1, int(num_nodes_)+1):
        for j in range(0, int(num_values_)):
            line = 'node'+str(i)+';'+str(gauss(0.0, 1.0))+'\n'
            file_.write(line)

def one_file_per_basestation(num_nodes_, num_values_, num_basestations_):
    base_file_name = '../resources/node_values/per_base_'+str(num_basestations_)+'/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)




def run_mode(mode_, num_nodes_, num_values_):
    return {
        1: create_single_file(num_nodes_, num_values_),
        2: create_file_per_node(num_nodes_, num_values_),
        3: one_line_node_id_value(num_nodes_, num_values_)
    }.get(mode_, None)


mode, num_nodes, num_values = sys.argv[1], sys.argv[2], sys.argv[3]
run_mode(mode, num_nodes, num_values)






