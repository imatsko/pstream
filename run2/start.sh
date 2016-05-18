#!/bin/bash

MAIN_PATH=run2/run.go

BOOTSTRAP=$1
RATE=$2

PORT=$3

MAX_NODES=$4

COUNTER=0

while [ $COUNTER -lt $MAX_NODES ]; do
  go run $MAIN_PATH --listen ":${PORT}" --bootstrap $BOOTSTRAP --rate $RATE &
  PIDS[$COUNTER]=$!
  let COUNTER=COUNTER+1
  let PORT=PORT+1
  #sleep 1
done

trap "kill ${PIDS[*]}" SIGINT

wait
