#!/bin/bash

#########################################
#
# This script is meant to be a generic
# helper to run timed tests
#
# Arguments:
#  ClusterId ProcId LoopSeconds InitScript LoopScript
#
#########################################

clusterId=$1
procId=$2
loopSecs=$3
initScript=$4
loopScript=$5

chmod a+x "$loopScript"

# document when and where we started
echo "Startup: `date`"
echo "Node: `uname -n`"
echo "Site: $GLIDEIN_Site"
(echo '------ env -------'; env) 1>&2


#do not leave anything in the main dir
mkdir work
cd work

# run the initialization
source "../$initScript"

# put in place a protection so it will not run more than twice what expected
# Twice becuse each test script could run up to the maximum loopSecs
let "loopLimit=$loopSecs * 2"
(sleep $loopLimit; echo "Timeout reached"; kill $$)&
timeoutPID=$!

#env
failures=0
successes=0

tstart=`date +%s`
let tend=$tstart+$loopSecs
tnow=`date +%s`
while [ "$tnow" -lt "$tend" ]; do
 "../$loopScript"
 res=$?
 if [ $res -eq 0 ]; then
  let successes++
 else
  let failures++
 fi
 tnow=`date +%s`
done
let duration=$tnow-$tstart

kill $timeoutPID

echo "Termination: `date`"
echo "Duration: $duration"
echo "Successes: $successes Failures: $failures"

cat > ../timed_loop_${clusterId}_${procId}.result << EOF
Site: $GLIDEIN_Site
LoopStart: $tstart
LoopSeconds: $loopSecs
Duration: $duration
Successes: $successes 
Failures: $failures
EOF

exit $failures

