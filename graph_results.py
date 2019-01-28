import sys 
import numpy as np

def graph_output(file_name):
    import matplotlib.pylab as pylab
    file_handle = open(file_name, 'r')
    file_data = file_handle.read()
    rows = file_data.split('\n')
    data = [
        [], # Concurrency level
        [], # Raw successes
        [], # Raw fails
        [], # Successes/total
    ]
    for row in rows:
        items = [ itm for itm in row.split(' ') if len(itm) > 0]
        if not 'Concurrency' in items:
            continue
        concurrency_level = int(items[3])
        total_success = int(items[-3])
        total_fails = int(items[-1])
        data[0].append(concurrency_level)
        data[1].append(total_success)
        data[2].append(total_fails)
        data[3].append(0)
    for idx in range(0, len(data[0])):
        data[3][idx] = data[1][idx] * 1.0/(data[1][idx] + data[2][idx])
    indices = range(0, len(data[0]))
    pylab.bar(indices, data[3], align='center', log=False)
    pylab.xticks(indices, data[0])
    pylab.ylabel('Success Rate Fration')
    pylab.title('LIGO Frame Read Success Rates at Rate Limit = 10 mbps')
    pylab.savefig(file_name + 'linear.png', dpi=400)
    pylab.clf()
    pylab.bar(indices, data[3], align='center', log=True)
    pylab.xticks(indices, data[0])
    pylab.ylabel('Success Rate Fration')
    pylab.title('LIGO Frame Read Success Rates at Rate Limit = 10 mbps')
    pylab.savefig(file_name + 'log.png', dpi=400)

if __name__ == "__main__":
    graph_output(sys.argv[1])