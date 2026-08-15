#!/bin/bash
PI="node0"

echo "Cross-compiling for ARM64 (this will take a minute)..."
sudo docker buildx build --platform linux/arm64 -t kv-server-arm64 -o type=docker,dest=kv-server-arm64.tar .

echo "Transferring to Raspberry Pi..."
scp kv-server-arm64.tar $PI:~/

echo "Executing remote update on the Pi..."
ssh $PI << 'EOF'
  sudo docker load -i kv-server-arm64.tar
  sudo docker stop rpi-server
  sudo docker rm rpi-server
  sudo docker run -d \
    -p 7878:7878 \
    -v ~/dev/data/kv-store:/app \
    --name rpi-server \
    kv-server-arm64
  echo "Update complete on Pi."
EOF

echo "Done."
