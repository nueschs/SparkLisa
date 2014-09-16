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


def create_simple_random_topology(num_nodes_, mode=0):
    matrix = [[int(x) for x in y] for y in numpy.zeros((num_nodes_, num_nodes_))]
    side_length = int(math.sqrt(num_nodes_))
    if mode == 0:
        for i in range(0, num_nodes_):
            if 1 in matrix[i]:
                continue
            eligible_neighbours = find_simple_eligible_neighbours(i, side_length, num_nodes_)
            random.shuffle(eligible_neighbours)
            matrix[i][eligible_neighbours[0]] = 1
            matrix[eligible_neighbours[0]][i] = 1
        write_matrix_bare(matrix, num_nodes_, 'sparse')
    elif mode == 1:
        for i in range(0, num_nodes_):
            for j in find_simple_eligible_neighbours(i, side_length, num_nodes_):
                matrix[i][j]=1
                matrix[j][i]=1
        write_matrix_bare(matrix, num_nodes_, 'dense')


def find_simple_eligible_neighbours(i, side_length, num_nodes_):
    eligible_neighbours = []
    is_top, is_right, is_bottom, is_left = is_border_point(i, side_length, num_nodes_)
    if not is_top: eligible_neighbours.append(i-side_length)
    if not is_bottom: eligible_neighbours.append(i+side_length)
    if not is_right: eligible_neighbours.append(i+1)
    if not is_left: eligible_neighbours.append(i-1)
    return eligible_neighbours

def create_random_topology(num_nodes_):
    if not math.sqrt(num_nodes_).is_integer():
        usage()
        sys.exit(0)

    side_length = int(math.sqrt(num_nodes_))
    matrix = create_random_connected_topology(num_nodes_, side_length)
    write_matrix_bare(matrix, num_nodes_, 'connected')

def find_eligible_node(nodes_edges, num_nodes_, side_length):
    eligible_nodes = {}

    for node in nodes_edges.keys():
        is_top, is_right, is_bottom, is_left = is_border_point(node, side_length, num_nodes_)

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


def write_matrix_bare(matrix, num_nodes_, prefix):
    file_name_base = '../resources/topology/'
    if not os.path.isdir(file_name_base): os.makedirs(file_name_base)
    file_name = file_name_base + 'topology_bare_' + prefix + '_' + str(num_nodes_) + '.txt'
    if os.path.isfile(file_name): os.remove(file_name)

    file_ = open(file_name, 'wb')

    for idx, row in enumerate(matrix):
        line = ''
        for x in row:
            line += str(x) + ','
        file_.write(line.rstrip().rstrip(',') + '\n')

    file_.close()

def is_border_point(node, side_length, num_nodes_):
    is_right = node % side_length == side_length - 1
    is_bottom = num_nodes_ - node <= side_length
    is_top = node < side_length
    is_left = node % side_length == 0

    return  is_top, is_right, is_bottom, is_left

#
# num_nodes = int(sys.argv[1])
# avg_degree = float(sys.argv[2]) if len(sys.argv) > 2 else 2.5
#
# create_random_topology(num_nodes, avg_degree)