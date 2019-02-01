#! /usr/bin/env python

import os
import subprocess
import random
import sys
import socket 
import time

def print_err(filename, location):
    import traceback
    if len(sys.exc_info()) >= 3:
        (e_type, e, trace) = sys.exc_info()[0:3]
        err_trace = traceback.format_exception(e_type, e, trace)
    else:
        err_trace = ['','Could not get trace!']
    print >> sys.stderr, 'Found error when in %s running.'%(location)
    print >> sys.stderr, 'Fail on host: %s'%(socket.gethostname())
    print >> sys.stderr, 'Timestamp: %s'%(str(time.localtime()))
    print >> sys.stderr, 'File: %s'%(filename)
    for msg in err_trace:
        print >> sys.stderr, msg 


def copy_test(src_file, dest_dir, kb_per_sec):
    print("Using file :   %s\n"%(src_file))
    if 'pilot_proxy' in os.listdir(os.getcwd()):
        os.environ['X509_USER_PROXY'] = os.path.join(os.getcwd(), 'pilot_proxy')
    else:
        os.environ['X509_USER_PROXY'] = os.path.join(os.getcwd(), '../pilot_proxy')
    #assert os.path.isfile(src_file), "File %s not found in directory %s\n%s"%(src_file, os.path.dirname(src_file), str(os.listdir(os.path.dirname(src_file))))
    #assert os.path.isdir(dest_dir), "Dir %s not found"%(dest_dir)
    bitrate_flag = '--bwlimit=%d'%(kb_per_sec)
    arg_list = ['rsync', bitrate_flag, src_file, dest_dir]
    result = subprocess.check_call(arg_list)
    dest_file = os.path.join(dest_dir, os.path.basename(src_file))
    assert os.path.isfile(dest_file)
    os.remove(dest_file)
    return result

def main(argv):
    
    # Parse the args 

    dest_dir = None 
    transfer_rate = None 
    source_list_path = None
    repetitions = 1

    print('Starting rsync test.')
    for flag in argv:
        if flag.endswith('.py'):
            continue
        if flag.startswith('--sourcelist='):
            source_list_path = flag[len('--sourcelist='):]
        elif flag.startswith('--rate='):
            transfer_rate = int(flag[len('--rate='):])
        elif flag.startswith('--tmpdir='):
            dest_dir = flag[len('--tmpdir='):]
        elif flag.startswith('--repeat='):
            repetitions = int(flag[len('--repeat='):])
        else:
            msg = 'Invalid flag : %s'%(flag)
            raise RuntimeError(msg)

    # Can also use the environment 
    if source_list_path is None: 
        source_list_path = os.environ['SOURCE_LIST_PATH']
    if transfer_rate is None:
        transfer_rate = int(os.environ['TRANSFER_RATE'])
    if dest_dir is None:
        dest_dir = os.environ['DEST_DIR']
    if repetitions is None:
        repetitions = os.environ['REPETITIONS']

    # Verify the args 

    if dest_dir is None:
        raise RuntimeError('Dest dir was not passed!')
    elif not os.path.isdir(dest_dir):
        raise RuntimeError('Could not find destination dir %s!'%(dest_dir))

    if transfer_rate is None:
        raise RuntimeError('Transfer rate was not passed!')
    elif transfer_rate < 0:
        raise RuntimeError('Transfer rate %d is invalid!'%(transfer_rate))

    if source_list_path is None:
        raise RuntimeError('Source list was not passed!')
    elif not os.path.isfile(source_list_path):
        raise RuntimeError('Could not find source list file %s!'%(source_list_path))
    
    if repetitions is None:
        raise RuntimeError('Repetitions was not passed!')
    elif repetitions < 0:
        raise RuntimeError('Repetitions %d is invalid!'%(repetitions))
    
    print('Args: %s %s %s %s'%(str(source_list_path), str(transfer_rate), str(dest_dir), str(repetitions)))
    # Run the copier
    file_list = open(source_list_path).read().split('\n')

    num_bad = 0
    for _idx in range(0, repetitions):
        src = random.choice(file_list)
        try:
            trial_result = copy_test(src, dest_dir, transfer_rate)
            if trial_result != 0:
                print >> sys.stderr, 'Fail on host: %s'%(socket.gethostname())
                print >> sys.stderr, 'Timestamp: %s'%(str(time.localtime()))
                num_bad += 1
        except:
            print_err(src, "repetition's for loop")
            num_bad += 1
        time.sleep(5)
    print('Successes: %d Failures: %d'%(repetitions - num_bad, num_bad))
    return num_bad

if __name__ == "__main__":
    print >> sys.stderr, 'Running on host: %s'%(socket.gethostname())
    print >> sys.stderr, 'Timestamp: %s'%(str(time.localtime()))
    try:
        main(sys.argv[1:])
    except:
        print_err('UNKNOWN', 'main')


