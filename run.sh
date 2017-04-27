#!/bin/bash

EXE=$(stack exec which raft-test-node)

PIDS=""

name="raft-test-node"

tmux new-session -s $name -d
tmux select-window -t $name:1

for peer in $(cat peers)
do
    tmux splitw -t $name -h -- $EXE $@ --listen ${peer} --peers peers 2> ${peer}.err
done

tmux select-layout -t $name tiled
tmux -2 attach-session -t $name
