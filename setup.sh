#!/bin/bash
set -e

# Remove existing .env file if it exists
# rm -f .env
rm -rf ./logs ./plugins ./config

# Stop and remove containers, networks, and volumes
docker compose down -v

docker compose build

# Create required Airflow directories
mkdir -p ./logs ./plugins ./config

# Write the current user's UID into .env\
echo "AIRFLOW_UID=$(id -u)" >> .env

# Start all services
echo "Starting Airflow services..."
docker compose up -d
