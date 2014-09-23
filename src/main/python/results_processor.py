import os
import numpy
import collections

def file_len(file_name):
    with open(file_name) as f:
        i = 0
        for i, l in enumerate(f):
            pass
    return i + 1

def read_durations(path):
    directories = [ os.path.join(path, name) for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)) ]
    batches = dict()

    for directory in directories:
        batches_with_data = list()
        results_dirs = [
            os.path.join(directory, 'results', name) for name in os.listdir(os.path.join(directory, 'results'))
            if os.path.isdir(os.path.join(directory, 'results', name)) and name.startswith('finalLisaValues')
        ]
        for dir_ in results_dirs:
            if len([name for name in os.listdir(dir_) if (name.startswith('part-') and file_len(os.path.join(dir_, name)) > 10)]) > 0:
                batches_with_data.append(os.path.split(dir_)[1].split('-')[1])
        batches[directory] = batches_with_data

    durations = dict()
    for run_dir, run_batches in batches.items():
        run_name = os.path.split(run_dir)[1]
        num_base_stations = run_name.split('_')[1]
        log_file_name = os.path.join(run_dir, 'sparklisa_spatial_{0}.log'.format(num_base_stations))
        with open(log_file_name, 'rb') as f:
            log_file = f.readlines()

        durations[run_name]= [line.split()[-2] for line in log_file if 'JobScheduler: Total delay' in line and line.split()[-5] in run_batches]

    return durations

def format_line(count, line_name, key, percentile, stdevs, averages, mins, maxs):
    line = '{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\n'.format(
        count,
        mins[key],
        percentile[0],
        percentile[1],
        percentile[2],
        maxs[key],
        averages[key],
        stdevs[key],
        line_name
    )
    return line


def get_file_lines(averages, stdevs, percentiles, mins, maxs):
    num_base_stations = ''
    count = 0
    index = 1
    lines = ['#idx\tmin\t25\t50\t75\tmax\tavg\tstdDev\ttitle\n']
    for run_name, percentile in percentiles.items():
        if num_base_stations != run_name.split('_')[1]:
            count = 0
        num_base_stations = run_name.split('_')[1]
        line_name = '{0}_{1}'.format(num_base_stations, count)
        line = format_line(index, line_name, run_name, percentile, averages, stdevs, mins, maxs)
        lines.append(line)
        count += 1
        index += 1
    return  lines

def get_combined_file_lines(averages, stdevs, percentiles, mins, maxs):
    lines = ['#idx\tmin\t25\t50\t75\tmax\tavg\tstdDev\ttitle\n']
    index = 1
    for num_bases, percentile in percentiles.items():
        line = format_line(index, num_bases, num_bases, percentile, stdevs, averages, mins, maxs)
        lines.append(line)
        index += 1
    return lines

def get_all_averages_lines(averages):
    lines = ['#base\tavg1\tavg2\tavg3\n']
    line = ''
    num_base_stations = ''
    for run_name, avg in averages.items():
        if num_base_stations != run_name.split('_')[1]:
            num_base_stations = run_name.split('_')[1]
            if line: lines.append(line+'\n')
            line = '{0}'.format(num_base_stations)
        line += '\t{0}'.format(avg)
    lines.append(line)
    return lines



def create_spatial_averages_file(path, out_path=''):
    if not out_path:
        out_path = path

    durations = read_durations(path)
    empty_logs = [n for n,x in durations.items() if len(x) == 0]
    averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    stdevs = collections.OrderedDict(sorted({n: numpy.std([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    percentiles = collections.OrderedDict(sorted({n: numpy.percentile([float(y) for y in x], [25, 50, 75]) for n,x in durations.items() if len(x) > 0}.items()))
    maxs = collections.OrderedDict(sorted({n: max([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    mins = collections.OrderedDict(sorted({n: min([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))

    lines = get_file_lines(averages, stdevs, percentiles, mins, maxs)
    with open(os.path.join(out_path, 'spatial.dat'), 'wb') as f:
        f.writelines(lines)

    durations_ordered = collections.OrderedDict(sorted(durations.items()))
    durations_combined = {}
    run = ''
    comb_durs = []
    for run_name, durs in durations_ordered.items():
        if run != run_name.split('_')[1]:
            run = run_name.split('_')[1]
            comb_durs = []
            durations_combined[run] = comb_durs
        comb_durs.extend(durs)

    combined_averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_stdevs = collections.OrderedDict(sorted({n: numpy.std([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_percentiles = collections.OrderedDict(sorted({n: numpy.percentile([float(y) for y in x], [25, 50, 75, 100]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_maxs = collections.OrderedDict(sorted({n: max([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_mins = collections.OrderedDict(sorted({n: min([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))

    combined_lines = get_combined_file_lines(combined_averages, combined_stdevs, combined_percentiles, combined_mins, combined_maxs)
    with open(os.path.join(out_path, 'spatial_comb.dat'), 'wb') as f:
        f.writelines(combined_lines)

    with open(os.path.join(out_path, 'spatial_all_avgs.dat'), 'wb') as f:
        f.writelines(get_all_averages_lines(averages))

    return empty_logs, averages, stdevs, percentiles


create_spatial_averages_file('/home/snoooze/Dropbox/unibnf/msc_thesis/results/spatial')