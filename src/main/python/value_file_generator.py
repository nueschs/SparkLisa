# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'
import sys
import os
from random import gauss


def create_single_file(num_nodes_, num_values_, _):
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


def create_file_per_node(num_nodes_, num_values_, _):
    base_file_name = '../resources/node_values/' + num_nodes_ + '/' + num_values_ + '/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)
    for i in range(0, int(num_nodes_)):
        file_ = open(base_file_name + str(i) + '.txt', 'wb')
        for j in range(0, int(num_values_)):
            file_.write(str(gauss(0.0, 1.0)) + ';\n')
        file_.close()


def one_line_node_id_value(num_nodes_, num_values_, _):
    base_file_name = '../resources/node_values/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)

    file_name = base_file_name + num_nodes_ + '_' + num_values_ + '_%.txt'
    count = 0
    while os.path.isfile(file_name.replace('%', str(count))):
        count += 1
    file_name = file_name.replace('%', str(count))
    file_ = open(file_name, 'wb')
    for i in range(1, int(num_nodes_) + 1):
        for j in range(0, int(num_values_)):
            line = 'node' + str(i) + ';' + str(gauss(0.0, 1.0)) + '\n'
            file_.write(line)


def one_file_per_basestation(num_nodes_, num_values_, num_base_stations_):
    base_file_name = '../resources/node_values/per_base_' + str(num_base_stations_) + '/'
    if not os.path.isdir(base_file_name):
        os.makedirs(base_file_name)

    if int(num_nodes_) % num_base_stations_ > 0:
        print("Number of nodes has to be a multiple of number of base stations")
        sys.exit(0)

    nodes_per_base = int(num_nodes_) / int(num_base_stations_)
    for i in range(0, num_base_stations_):
        station_dir = base_file_name+str(i+1)+'/'
        if not os.path.isdir(station_dir):
            os.makedirs(station_dir)

        file_name = station_dir + num_nodes_ + '_' + num_values_ + '_%.txt'
        count = 0
        while os.path.isfile(file_name.replace('%', str(count))):
            count += 1
        file_name = file_name.replace('%', str(count))

        file_ = open(file_name, 'wb')
        for j in range(i * nodes_per_base, (i + 1) * nodes_per_base):
            node_id = 'node' + str(j+1)
            for k in range(0, int(num_values_)):
                file_.write(node_id + ';' + str(gauss(0.0, 1.0)) + '\n')


def run_mode(mode_, num_nodes_, num_values_, num_base_stations_):
    return {
        '1': create_single_file,
        '2': create_file_per_node,
        '3': one_line_node_id_value,
        '4': one_file_per_basestation
    }.get(mode_, None)(num_nodes_, num_values_, num_base_stations_)

#
# mode, num_nodes, num_values = sys.argv[1], sys.argv[2], sys.argv[3]
# num_base_stations = int(sys.argv[4]) if mode == '4' else None
# run_mode(mode, num_nodes, num_values, num_base_stations)






