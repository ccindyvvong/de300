#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock

# Build Jupyter Docker image
echo "Creating Jupyter Docker image from dockerfile"
docker build -f dockerfile -t jupyter-image .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run --name jupyter-container \
	   -v ~/de300/lab4:/app/lab4 \
	   -p 8888:8888 \
	   jupyter-image
