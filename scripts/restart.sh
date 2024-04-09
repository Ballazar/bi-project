#!/bin/bash

# Stop all running containers
newgrp docker
echo "Stopping all Docker containers..."
sudo docker stop $(docker ps -aq)

# Wait for a few seconds to ensure all containers are stopped
sleep 5

# Start all containers
echo "Starting all Docker containers..."
sudo docker start $(docker ps -aq)
