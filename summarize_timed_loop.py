#!/bin/env python

import sys
import os,os.path

def get_dirs(work_dir):
    fnames=os.listdir(work_dir)
    concurency_dirs=[]
    for fname in fnames:
        if not os.path.isdir(os.path.join(work_dir,fname)):
            continue # looking for directories only
        if fname[:12]!='concurrency_':
            continue # not interesting
        concurency_dirs.append(fname)
    return concurency_dirs
\
def get_runs(concurency_dirs):
    runs={}
    for fname in concurency_dirs:
        arr=fname.split('_')
        run=int(arr[3])
        concurrency=int(arr[1])
        if not runs.has_key(run):
            runs[run]=[]
        runs[run].append(concurrency)
    return runs

def process_run(work_dir,run,concurrencies):
    concurrencies.sort()
    for cc in concurrencies:
        dirname=os.path.join(work_dir,"concurrency_%i_run_%i"%(cc,run))
        tot_files=0
        tot_succ=0
        tot_fail=0
        parse_fail=0
        for jid in range(cc):
            jfname=os.path.join(dirname,"job%i/concurrency_%i.out"%(jid,cc))
            try:
                fd=open(jfname,'r')
                try:
                    lines=fd.readlines()
                finally:
                    fd.close()
                arr=lines[-1].split()
                succ=int(arr[1])
                fail=int(arr[3])
                
                tot_files+=1
                tot_succ+=succ
                tot_fail+=fail
            except:
                # just warn
                parse_fail+=1
        if parse_fail>0: 
            print "Failed reading/parsing %i/%i files in %s"%(parse_fail,cc,os.path.join(dirname,"job%i"%jid))

        if tot_files>0:
            avg_succ=tot_succ/tot_files
            avg_fail=tot_fail/tot_files
            print "Run %i Concurrency %i\tAvg1 succ %i fail %i\tTotal succ %i fail %i"%(run,cc,avg_succ,avg_fail,tot_succ,tot_fail)
    return


###########################################################
# Functions for proper startup
def main(argv):
    work_dir=argv[1]
    if not os.path.isdir(work_dir):
        print "%s not a directory"
        sys.exit(1)
    concurrency_dirs=get_dirs(work_dir)
    runs=get_runs(concurrency_dirs)
    run_nrs=runs.keys()
    run_nrs.sort()
    for run in run_nrs:
        process_run(work_dir,run,runs[run])

if __name__ == "__main__":
    main(sys.argv)
