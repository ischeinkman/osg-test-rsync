import sys 
import numpy as np
import pandas as pd

def ecount_graph(file_name, date_part = ''):
    import matplotlib.pylab as plt
    df = pd.read_csv(file_name, sep='\s+')
    df.sort_values(['FullErrors', 'PartialErrors', 'Successes'], inplace=True)
    plt.title('Error rates at %s'%(date_part))
    data = df.values.transpose()
    names = data[0]
    successes = data[1]
    partial_fails = data[2]
    full_fails = data[3]

    total_fail_count = partial_fails + full_fails
    total_runs = total_fail_count + successes
    for idx in range(0, len(total_runs)):
        if total_runs[idx] <= 0:
            print('%d (%s): %d + %d => %d, %d + %d => %d'%(idx, names[idx], partial_fails[idx], full_fails[idx], total_fail_count[idx], total_fail_count[idx], successes[idx], total_runs[idx]))

    success_frac = successes/total_runs
    partial_frac = partial_fails/total_runs
    full_frac = full_fails/total_runs

    ind = range(0, len(names))
    sg = plt.bar(ind, success_frac, width = 0.3)
    pg = plt.bar(ind, partial_frac, bottom=success_frac, width=0.3)
    fg = plt.bar(ind, full_frac, bottom=success_frac + partial_frac, width=0.3)
    plt.xticks(ind, names, rotation='vertical', fontsize='small')
    plt.ylabel('Fractional Rate')
    plt.legend((sg[0], pg[0], fg[0]), ('Success', 'Some errors', 'Complete failure'))

    plt.tight_layout()
    plt.savefig('%s.err_rates.png'%(file_name))



def graph_output(file_name, date_part = ''):
    import matplotlib.pylab as pylab
    file_handle = open(file_name, 'r')
    file_data = file_handle.read()
    rows = file_data.split('\n')
    data = [
        [], # Concurrency level
        [], # Raw successes
        [], # Raw fails
        [], # Successes/total
        [],
        [],
    ]
    for row in rows:
        items = [ itm for itm in row.split(' ') if len(itm) > 0]
        if not 'Concurrency' in items:
            continue
        concurrency_level = int(items[3])
        totals_idx = items.index('Total')
        successes_idx = totals_idx + items[totals_idx:].index('succ') + 1
        fails_idx = totals_idx + items[totals_idx:].index('fail') + 1
        total_success = int(items[successes_idx])
        total_fails = int(items[fails_idx])
        total_no_report = 0
        if 'no_report' in items:
            total_no_report = int(items[items.index('no_report') + 1])
        data[0].append(concurrency_level)
        data[1].append(total_success)
        data[2].append(total_fails)
        data[3].append(0)
        data[4].append(total_no_report)
    for idx in range(0, len(data[0])):
        data[3][idx] = data[1][idx] * 1.0/(data[1][idx] + data[2][idx])
    indices = range(0, len(data[0]))
    pylab.bar(indices, data[3], align='center', log=False)
    pylab.xticks(indices, data[0])
    pylab.ylabel('Success Rate Fration')
    if date_part is not None and len(date_part) > 0:
        pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s\n(%s)'%(date_part))
    else:
        pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s')
    pylab.savefig(file_name + 'linear.png', dpi=400)
    pylab.clf()
    pylab.bar(indices, data[3], align='center', log=True)
    pylab.xticks(indices, data[0])
    pylab.ylabel('Success Rate Fration')
    if date_part is not None and len(date_part) > 0:
        pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s\n(%s)'%(date_part))
    else:
        pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s')
    pylab.savefig(file_name + 'log.png', dpi=400)

    if any(map(lambda k : k > 0, data[4])):
        pylab.clf()
        for idx in range(0, len(data[0])):
            data[3][idx] = data[1][idx] * 1.0/(data[1][idx] + data[2][idx] + data[4][idx])
        pylab.bar(indices, data[3], align='center', log=False)
        pylab.xticks(indices, data[0])
        pylab.ylabel('Success Rate Fration')
        if date_part is not None and len(date_part) > 0:
            pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s\n(%s)'%(date_part))
        else:
            pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s')
        pylab.savefig(file_name + 'linear_noreport_fail.png', dpi=400)
        pylab.clf()
        pylab.bar(indices, data[3], align='center', log=True)
        pylab.xticks(indices, data[0])
        pylab.ylabel('Success Rate Fration')
        if date_part is not None and len(date_part) > 0:
            pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s\n(%s)'%(date_part))
        else:
            pylab.title('LIGO Frame Read Success Rates at Rate Limit = 1 MB/s')
        pylab.savefig(file_name + 'log_noreport_fail.png', dpi=400)
        rates = [data[4][idx]/float(data[0][idx]) for idx in range(0, len(data[0]))]
        pylab.clf()
        pylab.bar(indices, rates, align='center', log=False)
        pylab.xticks(indices, data[0])
        pylab.ylabel('No Report Rate Fration')
        pylab.title('No Report/Concurrency Level')
        pylab.savefig(file_name + 'noreport.png', dpi=400)

        
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
    print(flags)
    if not 'date_part' in flags[2]:
        flags[2]['date_part'] = ''
    if 'simple' in flags[1] or 'simple' in flags[2]:
        ecount_graph(flags[0][0], date_part = flags[2]['date_part'])
    else:
        graph_output(flags[0][0], date_part = flags[2]['date_part'])