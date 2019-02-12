
import os
import sys 

class FileTest:
    def __init__(self, concurrency, start_time, run, job, filenames, host, error_data):
        self.concurrency = concurrency
        self.run = run
        self.job = job 
        self.filenames = filenames
        self.host = host 
        self.start_time = start_time
        self.error_data = error_data
    def __repr__(self):
        return "\{concurrency = %s, run = %s, job = %s, filenames = %s, host = %s, start_time = %s, error_data = %s,\}"%(self.concurrency, self.run, self.job, self.filenames, self.host, self.start_time, self.error_data)
    def has_errors(self):
        len(self.error_data) != 0
def parse_job(path):
    (concurrency, run, job) = parse_path_info(path)


    stdout_file = 'concurrency_%d.out'%concurrency

    stdout_path = os.path.join(path, stdout_file)
    if not os.path.isfile(stdout_path):
        return FileTest(concurrency, 'E', run, job, ['E'], 'E', [('Job is currently running', 'E')])
    stdout_fl = open(stdout_path, 'r')
    stdout_data = stdout_fl.read()
    stdout_fl.close()
    if len(stdout_data) < 3:
        return FileTest(concurrency, 'E', run, job, ['E'], 'E', [('Output is empty', 'E')])

    stdout_lines = stdout_data.split('\n')
    FILE_LINE_PREFIX = 'Using file :   '
    filenames = [ln[len(FILE_LINE_PREFIX) : ] for ln in stdout_lines if ln.startswith(FILE_LINE_PREFIX)]

    stats_line = [ln for ln in stdout_lines if ln.startswith('Success')][0].split(' ')
    successes = int(stats_line[1])
    failures = int(stats_line[3])


    stderr_file = 'concurrency_%d.err'%concurrency
    stderr_path = os.path.join(path, stderr_file)
    if not os.path.isfile(stderr_path):
        return FileTest(concurrency, 'E', run, job, filenames, 'E', [('Stderr is empty', 'E')])
    stderr_fl = open(stderr_path, 'r')
    stderr_data = stderr_fl.read()
    stderr_fl.close()

    stderr_lines = stderr_data.split('\n')
    host_line = [ln for ln in stderr_lines if ln.startswith('Running on host: ')]
    host = host_line[0][len('Running on host: '):]

    timestamp_lines = [ln[len('Timestamp: '):] for ln in stderr_lines if ln.startswith('Timestamp: ')]
    assert(len(timestamp_lines) == 1 + failures)
    start_time = timestamp_lines[0]

    err_files = []
    for ln_idx in range(1, len(stderr_lines)):
        ln = stderr_lines[ln_idx]
        if ln.startswith('File: ') and stderr_lines[ln_idx - 1].startswith('Timestamp: '):
            err_files.append(ln[len('File: '):])

    error_msgs = []
    current_msg = ''
    should_skip = True
    for ln in stderr_lines: 
        if len(ln) < 2 or ln.startswith('Running on host') or ln.startswith('Fail on host') or ln.startswith('Timestamp: ') or ln.startswith('File: ') or ln.startswith('INFO') or ln.startswith('WARNING'):
            continue 
        elif ln.startswith('Found error '):
            if len(current_msg) > 0 and not should_skip:
                error_msgs.append(current_msg)
            should_skip = False
            current_msg = ''
        else:
            current_msg += '\n' + ln 
    if len(current_msg) > 0 and not should_skip:
        error_msgs.append(current_msg)
    assert len(error_msgs) == failures, "Mismatching messages: %d vs %d\nMessages: %s"%(len(error_msgs), failures, str(error_msgs))
    error_data = []
    for idx in range(0, failures):
        msg = error_msgs[idx]
        timestamp = timestamp_lines[idx + 1]
        file = None
        if len(err_files) != 0:
            file = err_files[idx]
        error_data.append((msg, timestamp, file))
    return FileTest(concurrency, start_time, run, job, filenames, host, error_data)

def parse_path_info(path):
    if path[-1] == '/':
        path = path[:-1]
    (concrun_path, job_folder) = os.path.split(path)
    jobnum = int(job_folder[3:])
    (root_path, concrun_folder) = os.path.split(concrun_path)
    (concurrency, run) = [int(val_part) for val_part in concrun_folder.split('_')[1::2]]
    return (concurrency, run, jobnum)

def parse_run(run_path):
    (concurrencies, runs) = parse_concrun_data(run_path)
    concurrency_folders = [folder for folder in os.listdir(run_path) if folder.startswith('concurrency_')]
    concurrencies = remove_duplicates([int(folder.split('_')[1]) for folder in concurrency_folders])
    runs = remove_duplicates([int(folder.split('_')[3]) for folder in concurrency_folders])
    all_data = []
    for conc in concurrencies:
        all_data.append([])
        for rn in runs:
            all_data[-1].append([])
            base_path = os.path.join(run_path, 'concurrency_%d_run_%d/'%(conc, rn))
            for jobnum in range(0, conc):
                job_path = os.path.join(base_path, 'job%d/'%(jobnum))
                all_data[-1][-1].append(parse_job(job_path))
    return all_data

def remove_duplicates(inlist):
    retval = []
    for itm in inlist:
        if not itm in retval:
            retval.append(itm)
    return retval

def parse_concrun_data(run_path):
    concurrency_folders = [folder for folder in os.listdir(run_path) if folder.startswith('concurrency_')]
    concurrencies = remove_duplicates([int(folder.split('_')[1]) for folder in concurrency_folders])
    runs = remove_duplicates([int(folder.split('_')[3]) for folder in concurrency_folders])
    return (concurrencies, runs)

def tests_by_files(run_data, only_with_errors = False):
    retval = {}
    for dt in run_data:
        for fln in dt.filenames:
            if not fln in retval:
                retval[fln] = []
            retval[fln].append(dt)

    if only_with_errors:
        nret = {}
        for fln in retval:
            if any(map(lambda itm : len(itm.error_data) > 0, retval[fln])):
                nret[fln] = retval[fln]
        return nret
    else:
        return retval
def tests_by_host(run_data):
    retval = {}
    for dt in run_data:
        if not dt.host in retval:
            retval[dt.host] = []
        retval[dt.host].append(dt)
    return retval
def tbf_to_err_counts(tbf_data):
    retval = {}
    for file_name in tbf_data.keys():
        retval[file_name] = [0, 0, 0]
        for run in tbf_data[file_name]:
            if len(run.error_data) == 0:
                retval[file_name][0] += 1
            elif len(run.error_data) == len(run.filenames):
                retval[file_name][2] += 1
            else:
                should_use = None 
                if run.host == file_name:
                    should_use = True
                for dt in run.error_data:
                    if should_use is True:
                        break
                    elif dt[2] == file_name:
                        should_use = True
                    elif should_use is None and dt[2] is not None:
                        should_use = False 
                if should_use is not False:
                    retval[file_name][1] += 1
    return retval

def parse_flag(flag):
    if not flag.startswith('--'):
        return flag 
    if not '=' in flag:
        return flag[2:]
    val_start = flag.index('=')
    key = flag[2:val_start]
    val = flag[val_start + 1:]
    return (key, val)

def parse_flags(args):
    unnamed_list = []
    unvalued_list = []
    flag_pairs = {}
    for flag in args:
        flag_out = parse_flag(flag)
        if flag_out == flag:
            unnamed_list.append(flag_out)
        elif type(flag_out) == type(''):
            unvalued_list.append(flag_out)
        else:
            flag_pairs[flag_out[0]] = flag_out[1]
    return (unnamed_list, unvalued_list, flag_pairs)

if __name__ == "__main__":
    flags = parse_flags(sys.argv[1:])

    file_to_use = flags[0][0]
    all_data = parse_run(file_to_use)
    out_style = 'file_err_counts'
    if 'outstyle' in flags[2]:
        out_style = flags[2]['outstyle']

    if 'file' in out_style:
        print("Filename Successes PartialErrors FullErrors")
        flattened_data = all_data
        while type(flattened_data[0]) == type([]):
            flattened_data = reduce(lambda a,b : a + b, flattened_data, [])
        tbf = tests_by_files(flattened_data)
        err_counts = tbf_to_err_counts(tbf)
        for file_name in err_counts:
            print('%s %d %d %d'%( file_name, err_counts[file_name][0], err_counts[file_name][1], err_counts[file_name][2]))
    elif 'host' in out_style:
        print("Host Successes PartialErrors FullErrors")
        flattened_data = all_data
        while type(flattened_data[0]) == type([]):
            flattened_data = reduce(lambda a,b : a + b, flattened_data, [])
        tbf = tests_by_host(flattened_data)
        err_counts = tbf_to_err_counts(tbf)
        for file_name in err_counts:
            print('%s %d %d %d'%( file_name, err_counts[file_name][0], err_counts[file_name][1], err_counts[file_name][2]))

    elif 'run' in out_style:
        print('Concurrency Successes PartialErrors FullErrors')
        concurrencies, _ = parse_concrun_data(file_to_use)
        for idx in range(0, len(all_data)):
            conc_name = str(concurrencies[idx])
            partial_errs = 0
            full_errs = 0
            successes = 0
            for rn in all_data[idx]:
                for jobdata in rn:
                    if len(jobdata.error_data) == 0:
                        successes += 1
                    elif len(jobdata.error_data) == len(jobdata.filenames):
                        full_errs += 1
                    else:
                        partial_errs += 1
            print("%s %d %d %d"%( conc_name, successes, partial_errs, full_errs))
    else:
        print('Bad style %s'%(out_style))

