# !/usr/bin/env python

import argparse
import shutil
import time
import tarfile
import os
import subprocess
import shlex
from datetime import datetime

from snakebite.client import Client


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
spark_bin_path = '/home/stefan/spark/bin/'
spark_command = spark_bin_path+'spark-submit --class ch.unibnf.mcs.sparklisa.app.{0} --master yarn-client --num-executors ' \
                '{1} ../../../target/SparkLisa-0.0.1-SNAPSHOT.jar {2} {3} {4} {5} ' \
                '../resources/topology/topology_bare_{6}_1600.txt {7}'
log_file_path = '../resources/logs'

date_format = '%d%m%Y%H%M%S'


def parse_arguments():
    parser = argparse.ArgumentParser('Cluster automation for SparkLisa')
    parser.add_argument('rate', metavar='r', type=int, help='Number of values per minute submitted to each node')
    parser.add_argument('window', metavar='w', type=int, help='Window duration in seconds')
    parser.add_argument('duration', metavar='d', type=int, help='Duration after which the Spark Job is terminated')

    args = vars(parser.parse_args())

    global rate, window, duration
    rate = args['rate']
    window = args['window']
    duration = args['duration']


def delete_folder_contents(path):
    for root, dirs, files in os.walk(path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def cleanup_hdfs(num_nodes, num_base_stations):
    hdfs_client.delete(['sparkLisa/results/{1}_{0}/'.format(num_nodes, num_base_stations)], recurse=True).next()


def collect_and_zip_output(log_file_name, num_base_stations, num_nodes, topology_type, run_type):
    output_folder = '../resources/temp/'
    if not os.path.isdir(output_folder): os.makedirs(output_folder)
    if not os.path.isdir(output_folder + 'results/'): os.makedirs(output_folder + 'results/')

    shutil.copyfile(log_file_name, output_folder + log_file_name.split('/')[-1])
    shutil.copy('../resources/topology/topology_bare_{0}_{1}.txt'.format(topology_type, num_nodes),
                '../resources/temp/')
    list(hdfs_client.copyToLocal(['sparkLisa/results/{0}_{1}'.format(num_base_stations, num_nodes) + '/'],
                                 output_folder + 'results/'))
    tar_name = '{0}_{1}_{2}_{3}_{4}_{5}_{6}'.format(run_type, num_base_stations, num_nodes, rate, window, duration,
                                                    datetime.now().strftime(date_format))
    create_tar('../resources/', tar_name, '../resources/temp')
    delete_folder_contents('../resources/temp/')
    delete_folder_contents('../resources/topology')
    cleanup_hdfs(num_nodes, num_base_stations)


def create_tar(tar_path, tar_name, path):
    tar_file = tarfile.open(os.path.join(tar_path, tar_name) + '.tar.gz', 'w:gz')
    tar_file.add(path, arcname=tar_name)
    tar_file.close()


def main():
    parse_arguments()

    if not os.path.isdir(log_file_path):
        os.makedirs(log_file_path)

    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'

    topology_type = 'connected'
    for num_base in [1]:
        spark_command_ = spark_command.format(
            'SparkLisaStreamingJob',
            num_base * 2,
            window,
            rate,
            num_base,
            duration,
            topology_type,
            ''
        )
        log_file_name = 'sparklisa_spatial_{0}.log'.format(num_base)
        log_file = os.path.join(log_file_path, log_file_name)
        p = subprocess.Popen(shlex.split('script ' + log_file), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             shell=False)
        p.stdin.write(spark_command_ + '\n')
        time.sleep(duration + 60)
        p.stdin.write('exit\n')
        collect_and_zip_output(log_file, num_base, 1600, topology_type, 'spatial')


main()