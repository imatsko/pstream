#!/bin/bash

MAIN_PATH=run2/run2
chmod a+x $MAIN_PATH

LOG_DIR="./log"
mkdir -p "./log"

BASE_BOOTSTRAP_PORT=9000
BASE_BOOTSTRAP="127.0.0.1:${BASE_BOOTSTRAP_PORT}"

TYPE=$1

if [ $TYPE == "-h" ]; then
   echo "$0 source <PORT> <RATE>"
   echo "$0 peer <BASE_BOOTSTRAP> <START_PORT> <COUNT>"
   exit 0
elif [ $TYPE == "source" ]; then
    BASE_BOOTSTRAP_PORT=$2
    RATE=$3

    FREQUENCY=10
    CHUNKS_COUNT=5000
    BITRATE=1
    PEER_ID="source"

    CMD="./$MAIN_PATH --listen :${BASE_BOOTSTRAP_PORT} --source --chunks ${CHUNKS_COUNT} --freq ${FREQUENCY} --bitrate ${BITRATE} --rate ${RATE} --id ${PEER_ID} 2>&1 | tee ${LOG_DIR}/${PEER_ID}.log"
    echo "$CMD"
    eval $CMD
    exit
fi

BASE_BOOTSTRAP=$2
PORT=$3
MAX_NODES=$4

PERIOD=0.5
COUNTER=1

while [ $COUNTER -le $MAX_NODES ]; do
    CLASS=0
    RATE=10

    RANGE=100
    number=$RANDOM
    let "number %= $RANGE"
    if [ $number -le 10 ]; then
        CLASS=1
        RATE=5
    elif [ $number -le 50 ]; then
        CLASS=2
        RATE=1
    elif [ $number -le 90 ]; then
        CLASS=3
        RATE=0.5
    elif [ $number -le 100 ]; then
        CLASS=4
        RATE=0.1
    else
        CLASS=0
        RATE=20
    fi

    if [ $COUNTER -gt 5 ]; then
        p=$RANDOM
        let "p %= $COUNTER"
        PORT1="${PORTS[p]}"
        p=$RANDOM
        let "p %= $COUNTER"
        PORT2="${PORTS[p]}"
        p=$RANDOM
        let "p %= $COUNTER"
        PORT3="${PORTS[p]}"
        p=$RANDOM
        let "p %= $COUNTER"
        PORT4="${PORTS[p]}"
        p=$RANDOM
        let "p %= $COUNTER"
        PORT5="${PORTS[p]}"
        BOOTSTRAP="127.0.0.1:${PORT1},127.0.0.1:${PORT2},127.0.0.1:${PORT3},127.0.0.1:${PORT4},127.0.0.1:${PORT5}"
        if [ $COUNTER -lt 10 ]; then
            BOOTSTRAP="${BASE_BOOTSTRAP},${BOOTSTRAP}"
        fi
    else
        BOOTSTRAP="${BASE_BOOTSTRAP}"
    fi

    PEER_ID="peer_${CLASS}_${COUNTER}"

    CMD="./$MAIN_PATH --listen :${PORT} --bootstrap $BOOTSTRAP --rate $RATE  --id ${PEER_ID}  2>&1 | tee ${LOG_DIR}/${PEER_ID}.log &"
    echo "$CMD"

    eval "$CMD"
    PIDS[$COUNTER]=$!
    PORTS[$COUNTER]=$PORT
    let COUNTER=COUNTER+1
    let PORT=PORT+1
    sleep $PERIOD
done

#trap "kill ${PIDS[*]}" SIGINT
trap "pgrep run2 | xargs kill -9" SIGINT

wait
