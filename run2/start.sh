#!/bin/bash

MAIN_PATH=run2/run.go

PORT=9010

MAX_NODES=2
COUNTER=0
while [ $COUNTER -lt $MAX_NODES ]; do
  go run $MAIN_PATH --listen ":${PORT}" --bootstrap 127.0.0.1:9000 &
  let COUNTER=COUNTER+1
  let PORT=PORT+1
  sleep 1
done
