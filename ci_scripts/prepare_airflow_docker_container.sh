#!/bin/bash

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

echo "Starting airflow container"

docker compose -f docker-compose-test.yaml up -d --wait

echo "Waiting for airflow container to be ready"

response=$(curl -s http://localhost:8080/health)
current_try=1

while [[ ! ${response} =~ "\"status\": \"healthy\"" ]]; do
  echo "Airflow server is not ready yet... Try ${current_try}..."
  sleep 5

  response=$(curl -s http://localhost:8080/health)

  if [ ${current_try} -gt 40 ]; then
    echo 'Airflow server is not ready after 40 tries. Terminating...'
    docker-compose down -v
    make clear
    exit 1
  fi
  ((current_try++))
done
echo "Airflow server is ready"