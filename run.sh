#!/bin/bash

EXE=$(stack exec which raft-test-node)

PIDS=""

for peer in $(cat peers)
do
    echo "Running ${peer}"
    $EXE $@ --listen ${peer} --peers peers 2> ${peer}.err &
    PIDS="${PIDS} $!"
    sleep 0.2s
done

wait $PIDS
