#!/bin/bash
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
  --name local_node \
  kv-server

echo "Done. Follow logs with: sudo docker logs -f local_node"
