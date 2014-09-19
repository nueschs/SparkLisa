import numpy
import os, sys

TOLERANCE = 0.0000000001

def approx_Equal(x, y, tolerance=TOLERANCE):
    return x == y or abs(x-y) <= 0.5 * tolerance * (abs(x) + abs(y))

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


def read_single_file_values(file_name):
    with open(file_name, 'rb') as f:
        node_values = f.readlines()

    node_value_map = dict()
    for line in node_values:
        node_id, val = line.strip().strip('()').split(',')
        node_value_map[node_id] = float(val)

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
            expected_results[cnt][node_id] = round(lisa_val*neighbour_val, 9)

    return expected_results

def calculate_expected_results_single(node_values, node_map):
    stddev = numpy.std([x for x in node_values.values()])
    mean = numpy.average([x for x in node_values.values()])
    lisa_values = {node_id: (val-mean)/stddev for node_id, val in node_values.items()}

    expected_results = dict()
    for node_id, lisa_val in lisa_values.items():
        neighbours = node_map[node_id]
        neighbour_val = 0
        for neighbour in neighbours:
            neighbour_val += lisa_values[neighbour]/len(neighbours)
        expected_results[node_id] = lisa_val*neighbour_val

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
        spark_results[cnt][nodeid] = round(float(lisa_val), 9)

    return spark_results

def parse_results_single_file(file_name):
    with open(file_name, 'rb') as f:
        results_lines = f.readlines()

    spark_results = dict()
    for line in results_lines:
        node_id, lisa_val = line.strip().strip('()').split(',')
        spark_results[node_id] = float(lisa_val)

    return spark_results

def calculate_expected_positions(with_ids_file, node_values, results):
    with open(with_ids_file, 'rb') as f:
        lines = f.readlines()

    stddev = numpy.std([x for x in node_values.values()])
    mean = numpy.average([x for x in node_values.values()])

    random_lisas = dict()
    random_lisas_node1 = dict()

    for line in lines:
        line_str = line[:-2]
        node_id =line_str.split(',')[0].lstrip('(')
        lisa_val = (node_values[node_id]-mean)/stddev
        neighbours = ['node'+str(int(x.strip().lstrip('node'))+1) for x in line_str.rstrip(')').split('List(')[1].split(',')]
        # neighbours = [x.strip() for x in line_str.rstrip(')').split('List(')[1].split(',')]
        neighbour_lisas = [(node_values[x]-mean)/stddev for x in neighbours]
        neighbours_avg = sum(neighbour_lisas)/len(neighbour_lisas)

        if not node_id in random_lisas:
            random_lisas[node_id] = list()

        random_lisas[node_id].append(lisa_val*neighbours_avg)
        if node_id == 'node1':
            neistr = '('
            for nid in neighbours: neistr += nid+', '
            neistr = neistr.rstrip().rstrip(',')+')'
            random_lisas_node1[neistr]=lisa_val*neighbours_avg





    pos_map = dict()
    for node_id, res in results.items():
        if node_id == 'node1': random_lisas_node1 = random_lisas
        filtered_list = [x for x in random_lisas[node_id] if x < res]
        pos_map[node_id] = len(filtered_list)/float(len(random_lisas[node_id]))
    return pos_map, random_lisas_node1


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


def verify_results_stats(value_file, results_file, with_ids_file, stats_file, topology_file, test):
    node_values = read_single_file_values(value_file)
    node_map = create_node_map(topology_file)
    expected_results = calculate_expected_results_single(node_values, node_map)
    spark_results = parse_results_single_file(results_file)
    expected_positions, random_lisas_node1 = calculate_expected_positions(with_ids_file, node_values, expected_results)
    calculated_positions = parse_results_single_file(stats_file)
    test(expected_positions, test)
    sys.exit(0)

    bad_count = 0
    for node_id, lisa_val in spark_results.items():
        expected = expected_results[node_id]
        if not approx_Equal(expected, lisa_val):
            bad_count += 1
            print(node_id, expected, lisa_val)
    print('--------------------  Bad Count ------------------')
    print(bad_count)
    print('--------------------------------------------------\n')

    bad_count = 0
    for node_id, pos in calculated_positions.items():
        expected = expected_positions[node_id]
        if not approx_Equal(expected, pos):
            bad_count += 1
            print(node_id, expected, pos)
    print('--------------------  Bad Count Stats ------------------')
    print(bad_count)
    print('--------------------------------------------------------\n')


def test(list_left, list_right_path):
    list_right = dict()
    with open(list_right_path, 'rb') as f:
        data = f.readlines()
    for line in data:
        node_id = line.lstrip('(').split(',')[0]
        if node_id != 'node1':
            continue
        nei_id = line.split('List')[1].split('),')[0]
        nei_sum = float(line.split(',')[-1].strip().rstrip(')'))
        list_right[nei_id]=nei_sum

    for k,v in list_left.items():
        good_count = 0
        if not k in list_right:
            print('Key Error: '+k)
        elif not approx_Equal(float(v), float(list_right[k])):
            print('differs', k, v, list_right[k])
        else:
            good_count += 1
        print('good_count', good_count)








import file_evaluator as ev

p = '../resources/temp/'

ev.verify_results_stats(p+'av', p+'flv', p+'lvwni', p+'mvp', p+'topology_bare_connected_16.txt', p+'rns')




