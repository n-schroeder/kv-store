#!/bin/bash
echo "Stopping and removing old container..."
sudo docker stop laptop-leader
sudo docker rm laptop-leader

echo "Rebuilding the Docker image..."
sudo docker build -t kv-server .

echo "Starting the new container..."
sudo docker run -d \
  -p 7878:7878 \
  -v ~/dev/data/kv-store:/app:Z \
  -e IS_LEADER=true \
  -e PEERS="192.168.1.50:7878" \
  --name laptop-leader \
  kv-server

echo "Done. Follow logs with: sudo docker logs -f laptop-leader"
