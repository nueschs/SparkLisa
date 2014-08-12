# -*- coding: utf-8 -*-
__author__ = 'Stefan Nüesch'
import sys, os
from random import gauss

num_nodes, num_values = sys.argv[1], sys.argv[2]

file_name = '../resources/node_values_'+str(num_nodes)+'_'+str(num_values)+'_%.txt'

count = 0

while os.path.isfile(file_name.replace('%', str(count))):
    count += 1

file_name = file_name.replace('%', str(count))
file_ = open(file_name, 'wb')

for i in range(0, int(num_values)):
    line = ''
    for j in range(0, int(num_nodes)):
        line += str(gauss(0.0, 1.0))+';'
    file_.write(line+'\n')
file_.close()


