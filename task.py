#! /usr/bin/env python

import os
import subprocess
import random
import sys

def copy_test(src_file, dest_dir, kb_per_sec):
    print("Using file :   %s\n"%(src_file))
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
    
    # Run the copier
    file_list = open(source_list_path).read().split('\n')

    for _idx in range(0, repetitions):
        src = random.choice(file_list)
        trial_result = copy_test(src, dest_dir, transfer_rate)
        if trial_result != 0:
            return trial_result
    return 0

if __name__ == "__main__":
    err_code = main(sys.argv[1:])
    if err_code != 0:
        raise RuntimeError(err_code)

