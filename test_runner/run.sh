#!/usr/bin/env bash
set -ex

DELAY=$1
OUTBAND=$2
BURST=10kb

echo "$@"
echo "DElay $DELAY"
echo "outband $OUTBAND"

#tc qdisc add dev eth0 root netem delay "$DELAY"
tc qdisc add dev eth0 root tbf rate "$OUTBAND" latency 15ms buffer 4k

#tc qdisc add dev eth0 root handle 1: htb default 1
#tc class add dev eth0 parent 1: classid 1:1 htb rate "$OUTBAND" ceil "$OUTBAND" burst 100k

#tc qdisc add dev eth0 root handle 1:0 netem delay "$DELAY"
#tc qdisc add dev eth0 parent 1:1 handle 10 tbf rate "$OUTBAND" latency 100ms buffer 10k

#tc qdisc add dev eth0 root netem delay "$DELAY" rate "$OUTBOUND"

exec /opt/run "${@:3}"



