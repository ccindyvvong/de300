!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock

# Build PostgreSQL Docker image
echo "Building PostgreSQL Docker image from dockerfile-postgresql"
docker build -f dockerfile -t image .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -v ~/de300/hw2:/app/hw2/ \
	   -p 8888:8888 \
	   image
