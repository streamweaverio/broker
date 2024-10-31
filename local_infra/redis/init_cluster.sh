#!/bin/sh

NODE_LIST=${1:-"node-1,node-2,node-3"}
PORT=${2:-6379}

sleep 10

# Convert the comma-separated list of nodes to an array
set -f
IFS=','
set -- $NODE_LIST
unset IFS
set +f

hosts=""

# Loop through the nodes and get their IP addresses
for node; do
    node_ip=$(getent hosts "$node" | awk '{ print $1 }')
    if [ -z "$node_ip" ]; then
        echo "Failed to resolve IP for node: $node"
        exit 1
    fi
    hosts="$hosts $node_ip:$PORT"
done

echo "Creating cluster with nodes: $hosts"

# Run the cluster creation command
redis-cli --cluster create $hosts --cluster-replicas 0 --cluster-yes