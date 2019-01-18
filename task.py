#!/usr/bin/env python3

import os
import subprocess
import random
import sys
from typing import Any, List

def copy_test(src_file : str, dest_dir : str, kb_per_sec : int) -> subprocess.CompletedProcess:
    assert(os.path.isfile(src_file))
    assert(os.path.isdir(dest_dir))
    bitrate_flag = '--bwlimit=%d'%(kb_per_sec)
    arg_list = ['rsync', bitrate_flag, src_file, dest_dir]
    result = subprocess.run(arg_list, capture_output=True)
    dest_file = os.path.join(dest_dir, os.path.basename(src_file))
    assert(os.path.isfile(dest_file))
    os.remove(dest_file)
    return result

def main(argv : List[str]) -> int:
    
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
        if trial_result.returncode != 0:
            return trial_result.returncode
    return 0

err_code = main(sys.argv)
if err_code != 0:
    raise RuntimeError(err_code)

