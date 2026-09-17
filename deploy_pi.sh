#!/bin/bash
set -e

PI_TARGET="node0"

echo "Cross-compiling for ARM64 (this will take a minute)..."
sudo docker buildx build --platform linux/arm64 -t kv-server-arm64 -o type=docker,dest=kv-server-arm64.tar .

echo "Transferring to Raspberry Pi..."
scp kv-server-arm64.tar $PI_TARGET:~/

echo "Executing remote update on the Pi..."
ssh $PI_TARGET << 'EOF'
  sudo docker load -i kv-server-arm64.tar

  sudo docker stop rpi-server || true
  sudo docker rm rpi-server || true

  # The Pi's own dialable address, as its peers would reach it: the node's Raft
  # identity for votedFor and leader redirects. Detected on the Pi itself.
  PI_NODE_ID="$(hostname -I | awk '{print $1}'):7878"
  echo "This node's ID: $PI_NODE_ID"

  sudo docker run -d \
    -p 7878:7878 \
    -v ~/dev/data/kv-store:/app \
    -e NODE_ID="$PI_NODE_ID" \
    --name rpi-server \
    kv-server-arm64
 
  echo "Update complete on Pi."
EOF

echo "Done."
