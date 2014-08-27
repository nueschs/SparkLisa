#!/usr/bin/env python

import argparse
import shutil
import os
import math
import time
import tarfile
import topology_creator as topo
import value_file_generator as vals
from multiprocessing import Process
from subprocess import call
from snakebite.client import Client
from datetime import datetime

avg_degree = 2.5
numbers_of_nodes = None
number_of_base_stations = None
rate = None
window = None
duration = None
number_of_values = None
number_of_files = None
hdfs_path = 'hdfs://diufpc56.unifr.ch:8020/user/stefan/sparkLisa/'
hdfs_client = Client('diufpc56.unifr.ch', 8020, use_trash=False)
# hdfs_path = 'hdfs://localhost:9999/sparkLisa/'
# hdfs_client = Client('localhost', 9999, use_trash=False)
duration_pos = 1
num_executor_pos = 10
topology_file_pos = 12
num_stations_pos = 13
window_pos = 14
spark_command = [
    'timeout',
    '',
    'spark-submit',
    '--class',
    'ch.unibnf.mcs.sparklisa.app.FileInputLisaStreamingJob',
    '--master',
    'yarn',
    '--deploy-mode',
    'client',
    '--num-executors',
    '',
    '../../../target/SparkLisa-0.0.1-SNAPSHOT.jar',
    '../resources/topology/topology_bare_{0}_2.5.txt',
    '',
    '',
]
log_file_path = '../resources/logs/'

date_format = '%d%m%Y%H%M%S'


def parse_arguments():
    parser = argparse.ArgumentParser('Cluster automation for SparkLisa')
    parser.add_argument('rate', metavar='r', type=int, help='Number of values per minute submitted to each node')
    parser.add_argument('window', metavar='w', type=int, help='Window duration in seconds')
    parser.add_argument('duration', metavar='d', type=int, help='Duration in seconds for which new values are submitted')
    parser.add_argument('-n', '--nodes', metavar='-n', nargs='*', default=[], type=int, help='List of number of nodes, one "run" is executed for each number')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-bs', '--basestations', metavar='bs', nargs='*', type=int, help='List of numbers af base stations. Has to match #nodes in length')
    group.add_argument('-b', '--basestation', metavar='b', type=int, help='Number of base stations. Constant for all runs.')

    args = vars(parser.parse_args())

    global rate, window, duration, numbers_of_nodes, number_of_base_stations, number_of_values, number_of_files
    rate = args['rate']
    window = args['window']
    duration = args['duration']
    numbers_of_nodes = args['nodes']
    number_of_base_stations = args['basestations'] if args['basestations'] is not None else args['basestation']
    number_of_values = (rate*window)/60
    number_of_files = int(math.ceil(float(duration)/window))


def create_topology(nodes_):
    topo.create_random_topology(nodes_, avg_degree)


def delete_folder_contents(path):
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def create_values(num_nodes):
    base_file_name = '../resources/node_values/per_base_{0}/'.format(number_of_base_stations)
    delete_folder_contents(base_file_name)
    for i in range(0, number_of_files):
        vals.one_file_per_basestation(num_nodes, number_of_values, number_of_base_stations)


def upload_values(num_files, num_values, num_nodes, num_base_stations, window_, initial_delay):
    base_path = '../resources/node_values/per_base_{0}/'.format(num_base_stations)
    if not os.path.isdir(base_path+'temp/'): os.makedirs(base_path+'temp/')
    hdfs_base_path = hdfs_path+'values/{0}_{1}/'.format(num_nodes, num_base_stations)
    for i in range(0, num_files):
        for j in range(0, num_base_stations):
            temp_path = base_path+'temp/{0}/'.format(j)
            os.mkdir(temp_path)
            hdfs_client.mkdir(['/user/stefan/sparkLisa/values/{0}_{1}/{2}'.format(num_nodes, num_base_stations, j+1)], create_parent=True).next()
            file_name = '{0}_{1}_{2}.txt'.format(num_nodes, num_values, i)
            shutil.copy(base_path+'/{0}/{1}'.format(j+1, file_name), temp_path)
        print('>>> uploading values: {0}'.format(i))
        call('hadoop fs -put '+ base_path+'temp/* '+hdfs_base_path, shell=True)
        delete_folder_contents(base_path+'temp')
        time.sleep(window_)
    os.remove(base_path+'temp')

def cleanup_hdfs(num_nodes, num_base_stations):
    hdfs_client.delete(['sparkLisa/values/{0}_{1}/'.format(num_nodes, num_base_stations)], recurse=True).next()
    hdfs_client.delete(['sparkLisa/results/{1}_{0}/'.format(num_nodes, num_base_stations)], recurse=True).next()

def collect_and_zip_output(log_file_name, num_base_stations, num_nodes):
    values_base_path = '../resources/node_values/per_base_{0}/'.format(num_base_stations)
    output_folder = '../resources/temp/'
    if not os.path.isdir(output_folder): os.makedirs(output_folder)
    if not os.path.isdir(output_folder+'results/'): os.makedirs(output_folder+'results/')

    shutil.copyfile(log_file_name, output_folder+log_file_name.split('/')[-1])
    shutil.copytree(values_base_path, output_folder+'node_values/')
    shutil.copy('../resources/topology/topology_bare_{0}_2.5.txt'.format(num_nodes), '../resources/temp/')
    list(hdfs_client.copyToLocal(['sparkLisa/results/{0}_{1}'.format(num_base_stations, num_nodes)+'/'], output_folder+'results/'))
    tar_name = '{0}_{1}_{2}_{3}_{4}_{5}'.format(num_base_stations, num_nodes, rate, window, duration, datetime.now().strftime(date_format))
    create_tar('../resources/', tar_name, '../resources/temp')
    delete_folder_contents('../resources/temp/')
    delete_folder_contents('../resources/node_values')
    delete_folder_contents('../resources/topology')
    cleanup_hdfs(num_nodes, num_base_stations)


def create_tar(tar_path, tar_name, path):
    tar_file =  tarfile.open(os.path.join(tar_path, tar_name)+'.tar.gz', 'w:gz')
    tar_file.add(path, arcname=tar_name)
    tar_file.close()


def main():
    parse_arguments()

    if not os.path.isdir(log_file_path):
        os.makedirs(log_file_path)

    for number_of_nodes in numbers_of_nodes:
        print('>>> creating values: {0}'.format(number_of_nodes))
        create_values(number_of_nodes)
        print('>>> creating topology: {0}'.format(number_of_nodes))
        create_topology(number_of_nodes)
        num_executors = number_of_base_stations if number_of_base_stations >= 16 else 16

        spark_command[topology_file_pos] = spark_command[topology_file_pos].format(number_of_nodes)
        spark_command[num_stations_pos] = str(number_of_base_stations)
        spark_command[duration_pos] = str(float(duration+60))
        spark_command[num_executor_pos] = str(num_executors)
        spark_command[window_pos] = str(window)
        spark_command_ = " ".join(spark_command)
        print('>>> uploading values')
        p = Process(target=upload_values, args=(number_of_files, number_of_values, numbers_of_nodes[0], number_of_base_stations, window, 20))
        p.start()
        print('>>> running spark')
        os.system(spark_command_)
        time.sleep(duration+60)
        p.join()
        log_file_name = log_file_path+'sparkLisa-job.log'
        collect_and_zip_output(log_file_name, number_of_base_stations, number_of_nodes)

main()