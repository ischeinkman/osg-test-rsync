
import sys
def parse_job_data(filename):
    retval = []
    fl_handle = open(filename, 'r')
    fl_data = fl_handle.read()
    fl_handle.close()

    fl_lines = fl_data.split('\n')
    idx = 0
    while idx < len(fl_lines):
        header = fl_lines[idx]
        if 'information event triggered' in header: 
            idx += 1
            newent = {}
            while idx < len(fl_lines) and '=' in fl_lines[idx]:
                k_untrimmed, v_untrimmed = fl_lines[idx].split('=', 1)
                k = k_untrimmed.strip()
                v = v_untrimmed.strip()
                newent[k] = v
                idx += 1
            retval.append(newent)
        elif 'evicted' in header:
            proc = int(header.split('.', 2)[1])
            retval.append({'Proc' : str(proc), 'Evicted' : 'True'})
        else:
            pass
        idx += 1
    return retval 

def data_rows_by_proc(rows):
    retval = {}
    for ent in rows:
        proc = ent['Proc']
        if not proc in retval:
            retval[proc] = {}
        for key in ent:
            if key == 'Proc':
                continue
            if not key in retval[proc]:
                retval[proc][key] = []
            retval[proc][key].append(ent[key])
    return retval

def slothosts_to_eviction_counts(by_proc_data):
    retval = {}
    for cur_proc in by_proc_data:
        data = by_proc_data[cur_proc]
        fail = 'Evicted' in data 
        for curslt in data['JOB_GLIDEIN_SiteWMS_Slot']:
            if not curslt in retval:
                retval[curslt] = [0, 0]
            if not fail:
                retval[curslt][0] += 1
            else:
                retval[curslt][1] += 1
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

def main(args):
    flags = parse_flags(args)
    if 'hostevicts' in flags[1]:
        file_names = flags[0]
        hostevict_data = {}
        for fn in file_names:
            proc_data = data_rows_by_proc(parse_job_data(fn))
            host_data = slothosts_to_eviction_counts(proc_data)
            for host in host_data:
                if not host in hostevict_data:
                    hostevict_data[host] = [0, 0]
                hostevict_data[host][0] += host_data[host][0]
                hostevict_data[host][1] += host_data[host][1]
        for host in hostevict_data:
            print('%s %d %d'%(host, hostevict_data[host][0], hostevict_data[host][1]))

    else:
        file_name = flags[0][0]
        data = data_rows_by_proc(parse_job_data(file_name))
        for itm in data:
            print('%s %s %s'%(itm, 'Evicted' in data[itm], data[itm]['JOB_GLIDEIN_SiteWMS_Slot']))

if __name__ == "__main__":
    main(sys.argv[1:])