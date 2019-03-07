
import os
import sys

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

def graph_csv_multi(file, primary_idx = 0, secondary_idx = -2, third_idx = -3):
    import matplotlib.pylab as plt
    import pandas as pd
    import time
    import numpy as np
    df = pd.read_csv(file)
    x_header = df.columns[primary_idx]
    y_header = 'KB/s'
    plt.xlabel(x_header)
    plt.ylabel(y_header)
    title = "Speedup Data %s"%(file[:-4])
    df.sort_values([x_header], inplace=True)
    all_data = df.values.transpose()
    x_data = all_data[primary_idx]
    y_data_a = all_data[secondary_idx]
    y_data_b = all_data[third_idx]
    ind = np.arange(0, len(x_data))
    plt.xticks(ind, x_data, rotation='vertical', fontsize='small')
    sga = plt.bar(ind, y_data_a, width=0.3, color='r')
    sgb = plt.bar(ind+0.3, y_data_b, width=0.3, color='b')
    plt.legend([sga[0], sgb[0]], (df.columns[secondary_idx], df.columns[third_idx]))
    plt.savefig("%s.png"%(file))

def main(args):
    if '--graph' in args:
        flname = [f for f in args if not f == '--graph'][0]
        graph_csv_multi(flname)
        return
    a = args[0]
    b = args[1]
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
