#!/bin/bash


# Navigate to Home Directory
cd airflow

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'
# Prepare Airflow Installation
mkdir -p ./logs ./plugins ./config 
echo -e "AIRFLOW_UID=$(id -u)" > .env

AIRFLOW_UID=50000


# Start Airflow
sudo docker compose up airflow-init
sudo docker compose up -d

# Download airflow.sh Utility Script
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/airflow.sh'
chmod +x airflow.sh

echo "Airflow setup is complete."
