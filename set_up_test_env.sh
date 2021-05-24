#!/usr/bin/env bash
mkdir -p $PWD/../Data_services_storage/
mkdir -p $PWD/../Data_services_storage/logs
mkdir -p $PWD/../Data_services_storage/airflow
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs

export PHAROS_HOST=localhost
export PHAROS_USER=root
export PHAROS_PASSWORD=faketestingpassword
export PHAROS_DATABASE=pharos67
export PYTHONPATH=$PWD

export AIRFLOW_HOME=$PWD/../Data_services_storage/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD
export AIRFLOW__CORE__LOAD_EXAMPLES=false

