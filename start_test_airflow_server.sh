#!/usr/bin/env bash
airflow db init
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
airflow webserver --port 8081