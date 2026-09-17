#!/bin/bash

# This node's own dialable address, as its peers would reach it. It's the node's
# Raft identity: what it records in votedFor and what it puts in a leader
# redirect. Auto-detected from the primary LAN address; override by exporting
# NODE_ID before running this script.
NODE_ID="${NODE_ID:-$(hostname -I | awk '{print $1}'):7878}"
echo "This node's ID: $NODE_ID"

echo "Stopping and removing old container..."
sudo docker stop local_node
sudo docker rm local_node

echo "Rebuilding the Docker image..."
sudo docker build -t kv-server .

echo "Starting the new container..."
sudo docker run -d \
  -p 7878:7878 \
  -v ~/dev/data/kv-store:/app:Z \
  -e PEERS="192.168.1.120:7878" \
  -e NODE_ID="$NODE_ID" \
  --name local_node \
  kv-server

echo "Done. Follow logs with: sudo docker logs -f local_node"
