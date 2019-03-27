
import os
import sys
from math import * 
def parse_history_file(file_data):
    retval = []
    cur_entry = None
    lines = file_data.split('\n') #List[str]
    for cur_ln in lines:
        if len(cur_ln.strip()) == 0:
            continue
        if cur_ln.startswith('***'):
            if cur_entry is not None and len(cur_entry) > 2:
                (t, c, r) = get_run_data(cur_entry)
                cur_entry['IlanTime'] = t
                cur_entry['IlanCon'] = c 
                get_file_names(cur_entry)
                get_read_bytes(cur_entry)
                retval.append(cur_entry)
            header_data = cur_ln.split(' = ')
            assert(header_data[0] == '*** Offset')
            offset = header_data[1].split(' ')[0].strip()
            cur_entry = {'Offset' : offset}
        elif cur_entry is not None:
            kv = cur_ln.split(' = ', 1)
            assert len(kv) == 2,'Bad kv %s'%(cur_ln) 
            k = kv[0].strip()
            v = kv[1].strip()
            assert(k not in cur_entry)
            cur_entry[k] = v
    if cur_entry is not None and len(cur_entry) > 2:
        cur_entry['IlanTime'] = t
        cur_entry['IlanCon'] = c 
        get_file_names(cur_entry)
        get_read_bytes(cur_entry)
        retval.append(cur_entry)
    return retval 

def get_run_data(data_entry):
    log = data_entry['UserLog']
    assert(log.startswith('"/home/ilan/osg-test-rsync/run_'))
    assert(log.endswith('.log"'))
    relevant_portion = log[len('"/home/ilan/osg-test-rsync/run_') :- len('.log"')]
    timestamp_details = relevant_portion.split('/')
    assert len(timestamp_details) == 2, 'Bad rp: %s'%(relevant_portion)
    timestamp, details = relevant_portion.split('/')
    _, con, _, run = details.split('_')
    return (
        timestamp,
        con, 
        run,
    )

def make_file_key(idx):
    return 'Used_File_' + str(idx)

def get_file_names(data_entry):
    out_parent = data_entry['Iwd'][1:-1]
    out_file_name = data_entry['Out'][1:-1]
    out_path = os.path.join(out_parent, out_file_name)
    out_file_handle = open(out_path, 'r')
    out_data = out_file_handle.read()
    cur_file_idx = 0
    for ln in out_data.split('\n'):
        if not ln.startswith('Using file'):
            continue
        used_file = ln.split(':', 1)[1].strip()
        file_key = make_file_key(cur_file_idx)
        data_entry[file_key] = used_file
        cur_file_idx += 1

def get_read_bytes(data_entry):
    total_bytes = 0
    cur_idx = 0
    while data_entry.has_key(make_file_key(cur_idx)):
        flname = data_entry[make_file_key(cur_idx)]
        total_bytes += os.path.getsize(flname)
        cur_idx += 1
    if total_bytes <= 0:
        return
    data_entry['Total_Read_Bytes'] = total_bytes
    cmtime = 0.0
    if data_entry.has_key('CommittedTime'):
        cmtime = float(data_entry['CommittedTime'])
    else:
        print('Bad committed!')
    if cmtime > 0.0:
        mb = total_bytes/(1024.0)
        data_entry['KB/s'] = mb/cmtime

def stratify_run_data(run_data, columns = ['IlanCon', 'ProcId'], counts_only = False):
    retval = {}
    for entry in run_data:
        if len(entry) == 3:
            continue
        subdata = retval
        for column in columns[:-1]:
            entvl = entry[column]
            if not entvl in subdata:
                subdata[entvl] = {}
            subdata = subdata[entvl]
        entvl = entry[columns[-1]]
        if counts_only:
            if not entvl in subdata:
                subdata[entvl] = 0
            subdata[entvl] += 1
        else:
            if not entvl in subdata:
                subdata[entvl] = []
            subdata[entvl].append(entry)
        assert(retval[entry[columns[0]]])
    return retval

def make_comparison_data(all_data, run_a, run_b, field = 'KB/s'):
    retval = {}

    run_a_data = []
    run_b_data = []
    for entry in all_data:
        if entry['IlanTime'] == run_a:
            run_a_data.append(entry)
        elif entry['IlanTime'] == run_b:
            run_b_data.append(entry)
    run_a_strats = stratify_run_data(run_a_data)
    run_b_strats = stratify_run_data(run_b_data)
    print('Concurrency,%s %s, %s %s, %s/%s %s'%(run_a, field, run_b, field, run_a, run_b, field))
    for lvl in run_a_strats:
        assert(run_b_strats.has_key(lvl))
        retval[lvl] = {}
        a_ents = []
        b_ents = []
        for pid in run_a_strats[lvl].keys():
            assert(run_b_strats[lvl].has_key(pid))
            a_ent = run_a_strats[lvl][pid][0]
            if a_ent.has_key(field):
                a_data = float(a_ent[field])
            else:
                a_data = 0.0
            if a_data > 0.0:
                a_ents.append(a_data)
            b_ent = run_b_strats[lvl][pid][0]
            if b_ent.has_key(field):
                b_data = float(b_ent[field])
            else:
                b_data = 0.0
            if b_data > 0.0:
                b_ents.append(b_data)
            if a_data > 0.0 and b_data > 0.0:
                comparison = a_data/b_data  
                retval[lvl][pid] = comparison
        if len(retval[lvl]) > 0:
            retval[lvl]['avg'] = sum(retval[lvl].values())/len(retval[lvl])
        else:
            retval[lvl]['avg'] = -1
        if len(a_ents) > 0:
            retval[lvl]['a'] = sum(a_ents)/len(a_ents)
        else:
            retval[lvl]['a'] = -1
        if len(b_ents) > 0:
            retval[lvl]['b'] = sum(b_ents)/len(b_ents)
        else:
            retval[lvl]['b'] = -1
        print('%s,%f,%f,%f'%(lvl, retval[lvl]['a'], retval[lvl]['b'], retval[lvl]['avg']))
    
def get_speed_data(all_data, run_name):
    retval = {}

    usable_entries = []
    site = ''
    for entry in all_data:
        if entry['IlanTime'] == run_name:
            usable_entries.append(entry)
            site = entry['MATCH_GLIDEIN_Site']
    print(site)
    stratified_entries = stratify_run_data(usable_entries)
    for outer in stratified_entries:
        retval[outer] = {'entries' : []}
        cummulative = 0.0
        count = 0
        for inner in stratified_entries[outer]:
            for entry in stratified_entries[outer][inner]:
                if not 'KB/s' in entry:
                    continue
                cummulative += float(entry['KB/s'])
                count += 1
                retval[outer]['entries'].append(float(entry['KB/s']))
        if count <= 0:
            print('Bad conc: %s'%(inner))
            continue
        retval[outer].update({'sum' : cummulative, 'avg' : cummulative/count, 'count' : count})
    return retval 



def graph_csv(file, primary_idx = 0, secondary_idx = -1):
    import matplotlib.pylab as plt
    import pandas as pd
    import time
    df = pd.read_csv(file)
    x_header = df.columns[primary_idx]
    y_header = df.columns[secondary_idx]
    plt.xlabel(x_header)
    plt.ylabel(y_header)
    title = "Speedup Data %s"%(file[:-4])
    df.sort_values([x_header], inplace=True)
    all_data = df.values.transpose()
    x_data = all_data[primary_idx]
    y_data = all_data[secondary_idx]
    print(x_data)
    print(y_data)
    ind = range(0, len(x_data))
    plt.xticks(ind, x_data, rotation='vertical', fontsize='small')
    sg = plt.bar(ind, y_data)
    plt.savefig("%s.png"%(file))

def graph_csv_multi(file, primary_idx = 0, y_list = [1, 2, 3]):
    import matplotlib.pylab as plt
    import pandas as pd
    import time
    import numpy as np
    valid_colors = ['r', 'b', 'g', 'c', 'm', 'k', 'y']
    df = pd.read_csv(file)
    x_header = df.columns[primary_idx]
    y_header = 'KB/s'
    plt.xlabel(x_header)
    plt.ylabel(y_header)
    title = "Speedup Data %s"%(file[:-4])
    df.sort_values([x_header], inplace=True)
    all_data = df.values.transpose()
    x_data = all_data[primary_idx]
    ind = np.arange(0, len(x_data))
    plt.xticks(ind, x_data, rotation='vertical', fontsize='small')
    legend_keys = []
    legend_bars = []
    for y_idx in range(0, len(y_list)):
        cur_data = all_data[y_list[y_idx]]
        sga = plt.bar(ind+0.3 * y_idx, cur_data, width=0.3, color=valid_colors[y_idx % len(valid_colors)])
        legend_bars.append(sga)
        legend_keys.append(df.columns[y_list[y_idx]])
    plt.legend(legend_bars, legend_keys)
    plt.savefig("%s.png"%(file))

def graph_muchdata(kwargs):
    import pandas as pd
    import numpy as np
    import matplotlib.pylab as plt

    if len(kwargs) and 'config_file' in kwargs:
        import json 
        fh = open(kwargs['config_file'], 'r')
        kwargs = json.load(fh)
        fh.close()

    files = kwargs['files']

    fig = plt.figure(figsize=(12,6),dpi=400)
    ax = fig.add_subplot(111)

    data_by_file = {}
    for fname in files:
        data_by_file[fname] = pd.read_csv(fname)
    
    x_header = kwargs.get('x_header') or list(data_by_file.values())[0].columns[0]
    plt.xlabel(x_header)
    x_data = list(data_by_file.values())[0][x_header]
    ind = np.arange(0, len(x_data))
    plt.xticks(x_data, rotation='vertical', fontsize='small')
    plt.ylabel(kwargs['y_label'])

    file_headers = kwargs['file_headers']
    file_colors = kwargs['file_colors']

    for y_header in kwargs['y_headers']:
        conf = kwargs.get(y_header) or {}
        for fname in data_by_file:
            x_data = data_by_file[fname][x_header]
            y_data = data_by_file[fname][y_header]
            data_label = '%s %s'%(file_headers[fname], y_header)
            linestyle = conf.get('line_style') or None 
            linecolor = file_colors[fname] or 'blue'
            ax.plot(x_data, y_data, label = data_label, linestyle=linestyle, color=linecolor)
    ax.legend(loc=(kwargs.get('legend_loc') or 0))
    outfile_name = kwargs.get('output') or ('++'.join(files) + '.png')
    plt.savefig(outfile_name)



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

def parse_run_list(raw):
    raw_list = raw.split(',')
    retval = []
    for itm in raw_list:
        timestamp_start = itm.find("2019")
        timestamp_end = timestamp_start + 15
        retval.append(itm[timestamp_start:timestamp_end])
    return retval

def get_combined_data(alldt, runlist):
    run_to_data = {}
    for run in runlist:
        run_to_data[run] = get_speed_data(alldt, run)
    run_to_data['avg'] = {}
    run_to_data['sum'] = {}
    run_to_data['count'] = {}
    run_to_data['entries'] = {}
    for run in runlist:
        for conc in run_to_data[run]:
            if not conc in run_to_data['sum']:
                run_to_data['sum'][conc] = 0.0
                run_to_data['count'][conc] = 0.0
                run_to_data['std'] = {}
                run_to_data['entries'][conc] = []
                run_to_data['avg'][conc] = 0.0
            run_to_data['sum'][conc] += run_to_data[run][conc]['sum']
            run_to_data['count'][conc] += run_to_data[run][conc]['count']
            run_to_data['entries'][conc] += run_to_data[run][conc]['entries']
    for conc in run_to_data['sum']:
        print('Calcing stats for conc %s'%(conc))
        ln = run_to_data['count'][conc]
        mean= run_to_data['sum'][conc]/ln
        run_to_data['avg'][conc] = mean

        sqerr_sum = 0.0
        sqerr_ln = 0
        second_mean = 0.0

        err_min = -1.0
        err_max = -1.0
        big_err = 0
        zro = 0
        run_to_data['entries'][conc].sort()
        for itm in run_to_data['entries'][conc]:
            sqerr_curr = (itm - mean) * (itm - mean)
            err_curr = sqrt(sqerr_curr)
            if err_max < err_curr or err_max < 0.0:
                err_max = err_curr
            if err_min > err_curr or err_min < 0.0:
                err_min = err_curr
            if err_curr >= mean:
                big_err += 1
            sqerr_sum += sqerr_curr
            second_mean += itm
            sqerr_ln += 1
        stddev = sqrt(sqerr_sum/sqerr_ln)
        print('Mean: %f vs %f'%(mean, second_mean/sqerr_ln))
        print("Max: %f, min: %f"%(run_to_data['entries'][conc][-1], run_to_data['entries'][conc][0]))
        print('STD: %f'%(stddev))
        print('Sum sqerr: %f, sqerr_len: %d (vs %d)'%(sqerr_sum, sqerr_ln, run_to_data['count'][conc]))
        print('Error min: %f, max: %f, big: %d, 0: %d'%(err_min, err_max, big_err, zro))
        run_to_data['std'][conc] = stddev
    return run_to_data

def median(lst):
    lst_len = len(lst)
    if lst_len % 2 == 1:
        med_idx = int(floor(lst_len/2))
        return lst[med_idx]
    else: 
        upper_idx = int(floor(lst_len/2))
        upper = lst[upper_idx]
        lower_idx = upper_idx - 1
        lower = lst[lower_idx]
        return (0.5 * lower) + (0.5 * upper)


def main(args):
    flags = parse_flags(args)
    if 'graph' in flags[1]:
        flname = [f for f in args if not f == '--graph'][0]
        graph_csv(flname)
        return
    elif 'graphmulti' in flags[1]:
        flname = flags[0][0]
        graph_csv_multi(flname)
        return

    elif 'sing' in flags[1]:
        flname = [f for f in args if 'history' in f.lower()][0]
        run = [f for f in args if '2019' in f.lower() and not 'history' in f.lower()][0]
        print('F: %s, R: %s'%(flname, run))
        f = open(flname, 'r')
        raw = f.read()
        f.close()
        print('Read file.')
        alldt = parse_history_file(raw)
        print('Parsed file.')
        rdt = get_speed_data(alldt, run)
        print('Built output.')
        for outer in rdt:
            avg = rdt[outer]['avg']
            print('%s, %f'%(outer, avg))
        return
    elif 'multi' in flags[1]:
        all_flnames = flags[2]['files'].split(',')
        all_runs = parse_run_list(flags[2]['runs'])
        alldt = []
        for flname in all_flnames:
            f = open(flname, 'r')
            raw = f.read()
            f.close()
            print('Read file %s.'%(flname))
            cur_dt = parse_history_file(raw)
            print('Parsed file %s.'%(flname))
            alldt += cur_dt
        parsed_dt = get_combined_data(alldt, all_runs)
        print('Concurrency,Mean,Stddev,Median,Min,Max')
        keys = map(lambda a: int(a), parsed_dt['avg'].keys())
        keys.sort()
        for concl in keys:
            conc = str(concl)
            print('%s,%f,%f,%f,%f,%f'%(
                conc, 
                parsed_dt['avg'][conc], parsed_dt['std'][conc], median(parsed_dt['entries'][conc]),
                parsed_dt['entries'][conc][0], parsed_dt['entries'][conc][-1], 
            ))
        return 
    elif 'newgraph' in flags[1]:
        json_file = flags[2]['config']
        graph_muchdata({'config_file' : json_file})
        return

    a = flags[0][0]
    b = flags[0][1]
    dt = []
    for orig in args[2:]:
        orig_f = open(orig, 'r')
        raw = orig_f.read()
        orig_f.close()
        ndt = parse_history_file(raw)
        dt += ndt
        print('Parsed %d (c: %d) entries from %s'%(len(ndt), len(dt), orig))
    rns = set([e['IlanTime'] for e in dt])
    print(rns)

    make_comparison_data(dt, a, b)
if __name__ == "__main__":
    main(sys.argv[1:])
# UserLog = "/home/ilan/osg-test-rsync/run_20190228_110028/con_2000_run_0.log"
