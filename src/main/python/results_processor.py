import os
import numpy
import collections
import warnings

warnings.simplefilter("error")

def file_len(file_name):
    with open(file_name) as f:
        i = 0
        for i, l in enumerate(f):
            pass
    return i + 1

def find_temporal_batches(path):
    batches_with_data = dict()
    num_nodes = int(os.path.split(path)[1].split('_')[3])
    k = int(os.path.split(path)[1].split('_')[2])
    results_dirs = [
        os.path.join(path, 'results', name) for name in os.listdir(os.path.join(path, 'results'))
        if os.path.isdir(os.path.join(path, 'results', name)) and name.startswith('allV')
    ]
    for dir_ in results_dirs:
        num_records = sum([
            file_len(os.path.join(dir_, x)) for x in os.listdir(dir_)
            if (x.startswith('part-') and file_len(os.path.join(dir_, x)) > 10)
        ])
        if 0 < num_records <= num_nodes*k:
            if not num_records in batches_with_data:
                batches_with_data[num_records] = list()
            batches_with_data[num_records].append(os.path.split(dir_)[1].split('-')[1])
    return {batch: key for key,batchs in batches_with_data.items() for batch in batchs },\
           {key:sorted(v) for key,v in batches_with_data.items()}

def read_temporal_durations(path):
    directories = [ os.path.join(path, name) for name in os.listdir(path) if os.path.isdir(os.path.join(path, name)) ]
    batches = dict()
    batches_reversed = dict()
    for directory in directories:
        batches_reversed[directory], batches[directory] = find_temporal_batches(directory)
    durations = dict()
    cnt = dict()
    for run_dir, run_batches in batches_reversed.items():
        num_base_stations = os.path.split(run_dir)[1].split('_')[-7]
        run_name = '{0}_0'.format(num_base_stations) if not num_base_stations in cnt else '{0}_{1}'.format(num_base_stations, cnt[num_base_stations])
        cnt[num_base_stations] = 1 if not num_base_stations in cnt else cnt[num_base_stations]+1
        k = os.path.split(run_dir)[1].split('_')[2]
        log_file_name = os.path.join(run_dir, 'sparklisa_{1}_{0}.log'.format(num_base_stations, 'temporal'))
        with open(log_file_name, 'rb') as f:
            log_file = f.readlines()

        if not k in durations:
            durations[k] = dict()

        run_durations = dict()
        for line in [x for x in log_file if 'JobScheduler: Total delay' in x and x.split()[-5] in batches_reversed[run_dir]]:
            duration = line.split()[-2]
            batch_name = line.split()[-5]
            t = run_batches[batch_name]
            if not run_batches[batch_name] in run_durations:
                run_durations[t] = dict()
            run_durations[t][batches[run_dir][t].index(batch_name)] = duration
        durations[k][run_name] = run_durations

    return durations

def get_durations_for_run(run_dir, run_batches, run_type, offset):
    num_base_stations = os.path.split(run_dir)[1].split('_')[offset]
    log_file_name = os.path.join(run_dir, 'sparklisa_{1}_{0}.log'.format(num_base_stations, run_type))
    with open(log_file_name, 'rb') as f:
        log_file = f.readlines()
    return [line.split()[-2] for line in log_file if 'JobScheduler: Total delay' in line and line.split()[-5] in run_batches]


def read_durations(path, run_type='spatial', prefix='finalLisaValues', bs_offset=-6):
    directories = [ os.path.join(path, name) for name in os.listdir(path)
                    if os.path.isdir(os.path.join(path, name)) and name.startswith(run_type)]
    batches = dict()

    for directory in directories:
        batches[directory] = get_batches_with_data(directory, prefix)

    durations = dict()
    for run_dir, run_batches in batches.items():
        run_name = os.path.split(run_dir)[1]
        durations[run_name]= get_durations_for_run(run_dir, run_batches, run_type, bs_offset)

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
        if num_base_stations != run_name.split('_')[-6]:
            count = 0
        num_base_stations = run_name.split('_')[-6]
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

    spatial_file_name = 'spatial_{0}.dat'
    comb_file_name = 'spatial_comb_{0}.dat'
    all_avgs_file_name = 'spatial_all_avgs_{0}.dat'

    if not os.path.split(path)[1].isdigit():
        spatial_file_name = spatial_file_name.format(1600)
        comb_file_name = comb_file_name.format(1600)
        all_avgs_file_name = all_avgs_file_name.format(1600)
    else:
        spatial_file_name = spatial_file_name.format(os.path.split(path)[1])
        comb_file_name = comb_file_name.format(os.path.split(path)[1])
        all_avgs_file_name = all_avgs_file_name.format(os.path.split(path)[1])

    with open(os.path.join(out_path, spatial_file_name), 'wb') as f:
        f.writelines(lines)

    durations_ordered = collections.OrderedDict(sorted(durations.items()))
    durations_combined = {}
    run = ''
    comb_durs = []
    for run_name, durs in durations_ordered.items():
        if run != run_name.split('_')[-6]:
            run = run_name.split('_')[-6]
            comb_durs = []
            durations_combined[run] = comb_durs
        comb_durs.extend(durs)

    combined_averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_stdevs = collections.OrderedDict(sorted({n: numpy.std([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_percentiles = collections.OrderedDict(sorted({n: numpy.percentile([float(y) for y in x], [25, 50, 75, 100]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_maxs = collections.OrderedDict(sorted({n: max([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_mins = collections.OrderedDict(sorted({n: min([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))

    combined_lines = get_combined_file_lines(combined_averages, combined_stdevs, combined_percentiles, combined_mins, combined_maxs)
    with open(os.path.join(out_path, comb_file_name), 'wb') as f:
        f.writelines(combined_lines)

    with open(os.path.join(out_path, all_avgs_file_name), 'wb') as f:
        f.writelines(get_all_averages_lines(averages))

    return empty_logs, averages, stdevs, percentiles

def create_monte_carlo_files(path, out_path='', run_type='monte_carlo'):
    if not out_path:
        out_path = path

    durations = read_durations(path, run_type, 'measuredValuesPositions')
    for _,x in durations.items(): print(len(x))
    empty_logs = [n for n,x in durations.items() if len(x) == 0]
    averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    stdevs = collections.OrderedDict(sorted({n: numpy.std([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    percentiles = collections.OrderedDict(sorted({n: numpy.percentile([float(y) for y in x], [25, 50, 75]) for n,x in durations.items() if len(x) > 0}.items()))
    maxs = collections.OrderedDict(sorted({n: max([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))
    mins = collections.OrderedDict(sorted({n: min([float(y) for y in x]) for n,x in durations.items() if len(x) > 0}.items()))

    lines = get_file_lines(averages, stdevs, percentiles, mins, maxs)
    with open(os.path.join(out_path, 'mc.dat'), 'wb') as f:
        f.writelines(lines)

    durations_ordered = collections.OrderedDict(sorted(durations.items()))
    durations_combined = {}
    run = ''
    comb_durs = []
    for run_name, durs in durations_ordered.items():
        if run != run_name.split('_')[-6]:
            run = run_name.split('_')[-6]
            comb_durs = []
            durations_combined[run] = comb_durs
        comb_durs.extend(durs)

    combined_averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_stdevs = collections.OrderedDict(sorted({n: numpy.std([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_percentiles = collections.OrderedDict(sorted({n: numpy.percentile([float(y) for y in x], [25, 50, 75, 100]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_maxs = collections.OrderedDict(sorted({n: max([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    combined_mins = collections.OrderedDict(sorted({n: min([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))

    combined_lines = get_combined_file_lines(combined_averages, combined_stdevs, combined_percentiles, combined_mins, combined_maxs)
    with open(os.path.join(out_path, 'mc_comb.dat'), 'wb') as f:
        f.writelines(combined_lines)

    with open(os.path.join(out_path, 'mc_all_avgs.dat'), 'wb') as f:
        f.writelines(get_all_averages_lines(averages))

def create_temporal_files(path, out_path=''):
    if not out_path:
        out_path = path

    durations = read_temporal_durations(path)

    histogram_dict = dict()
    print(durations)
    for k, runs in durations.items():
        if not k in histogram_dict:
            histogram_dict[k] = dict()
        for run, ts in runs.items():
            base = run.split('_')[0]
            if not base in histogram_dict:
                histogram_dict[base] = dict()
            avg = numpy.average([float(x) for y in ts.values() for x in y.values()])
            histogram_dict[k][base] = avg

    lines = ['#k 1 2 4 8 16 avg\n']
    print(histogram_dict)
    for k, bases in histogram_dict.items():
        if bases:
            line = '{0} {1} {2} {3} {4} {5} {6} \n'.format(k, bases['1'], bases['2'], bases['4'], bases['8'], bases['16'], numpy.average(bases.values()))
            lines.append(line)

    with open(os.path.join(out_path, 'temporal_histogram.dat'), 'wb') as f:
        f.writelines(lines)



def create_topologies_files(path, out_path=''):
    if not out_path:
        out_path = path
    durations = read_durations(path, 'topologies', 'measuredValuesPositions')
    durations_ordered = collections.OrderedDict(sorted(durations.items()))
    durations_combined = {}
    run = ''
    comb_durs = []
    for run_name, durs in durations_ordered.items():
        if run != run_name.split('_')[-7]:
            run = run_name.split('_')[-7]
            comb_durs = []
            durations_combined[run] = comb_durs
        comb_durs.extend(durs)

    combined_averages = collections.OrderedDict(sorted({n: numpy.average([float(y) for y in x]) for n,x in durations_combined.items() if len(x) > 0}.items()))
    lines = ['#type avg\n']
    for typ, avg in combined_averages.items():
        lines.append('{0} {1}\n'.format(typ, avg))
    with open(os.path.join(out_path, 'topologies_avgs.dat'), 'wb') as f:
        f.writelines(lines)


def get_batches_with_data(directory, prefix):
    batches_with_data = list()
    results_dirs = [
        os.path.join(directory, 'results', name) for name in os.listdir(os.path.join(directory, 'results'))
        if os.path.isdir(os.path.join(directory, 'results', name)) and name.startswith(prefix)
    ]
    for dir_ in results_dirs:
        if len([name for name in os.listdir(dir_) if (name.startswith('part-') and file_len(os.path.join(dir_, name)) > 10)]) > 0:
            batches_with_data.append(os.path.split(dir_)[1].split('-')[1])

    return batches_with_data

def create_single_run_graph_data(paths, run_type, prefix, out_path, out_file_name='test.dat'):
    lines = ['#cnt\t']
    columns = dict()
    for path in paths:
        batches = get_batches_with_data(path, prefix)
        durations = get_durations_for_run(path, batches, run_type)
        num_base_stations = os.path.split(path)[1].split('_')[-6]
        columns[num_base_stations] = durations

    cnt_base = 0
    for num_base, values in columns.items():
        cnt_base += 1
        cnt = 0
        lines[cnt] += '{0}\t'.format(num_base)
        for val in values:
            cnt += 1
            val_str = '{0}\t'.format(val)
            if cnt < len(lines):
                lines[cnt] += val_str
            else:
                line = '{0}'.format(cnt) + ''.join(['\t' for _ in range(0,cnt_base)])+val_str
                lines.append(line)
    lines = [line +'\n' for line in lines]

    with open(os.path.join(out_path, out_file_name), 'wb') as f:
        f.writelines(lines)

def create_mct_files(path, outpath):
    if not outpath:
        outpath = path

    for z in [{float(f):y.split('_')[5] for f in x} for y, x in read_durations(path, 'monte_carlo_temporal_local', 'measuredValuesPositions', -7).items()]:
        print(str(z).strip('{}').replace(':',''))

    # print(read_durations(path, 'monte_carlo_temporal_global', 'measuredValuesPositions', -7))











# create_spatial_averages_file('/home/snoooze/msctr/results/spatial/25600', '/home/snoooze/msctr/results/spatial/25600')
create_mct_files('/home/snoooze/msctr/results/mctl', '/home/snoooze/msctr/results/mctl')
# create_monte_carlo_files('/home/snoooze/msctr/results/monte_carlo/naive', run_type='monte_carlo_naive')
# create_topologies_files('/home/snoooze/msctr/results/topologies/10000')
# create_temporal_files('/home/snoooze/msctr/results/temporal')
# create_single_run_graph_data( [
#                                   '/home/snoooze/msctr/results/spatial/spatial_16_1600_20_3_1200_22092014124720',
#                                   '/home/snoooze/msctr/results/spatial/spatial_1_1600_20_3_1200_22092014084231',
#                                ],
#
#
#                               'spatial', 'finalLisaValues', '/home/snoooze/msctr/results/spatial', 'single_runs_spatial_1_16.dat')