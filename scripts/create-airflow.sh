#!/bin/bash

# Navigate to Home Directory
cd ~

# Prepare Airflow Installation
mkdir -p ~/airflow/logs ~/airflow/plugins ~/airflow/config 
echo -e "AIRFLOW_UID=$(id -u)" > ~/airflow/.env

# Navigate to Airflow Directory
cd ~/airflow

# Download Docker Compose YAML for Airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'

# Start Airflow
sudo docker compose up airflow-init
sudo docker compose up -d

# Download airflow.sh Utility Script
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/airflow.sh'
chmod +x airflow.sh

echo "Airflow setup is complete."
