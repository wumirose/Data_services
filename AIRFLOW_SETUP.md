# Setting Up Airflow locally (PROTOTYPE)

### Pip Install

Run 
```shell script
$ source <venv>/bin/activate
$ pip install -r requirements
```

### Setup airflow home dir

Setup where airflow would store config, database and logs when running locally.
This could be any dir that is writable by User that starts airflow services. 

```shell script
$ export AIRFLOW_HOME=<path_to_AIRFLOW_HOME>
```

### Init airflow DB and Configs

Run first bootstrap command to initialize db and config files.

```shell script
$ aiflow db init
```

### Edit some config

Open the `airflow.cfg`  config file and edit `dags_folder`. Set it's value to `<ABS_PATH_TO>/Data_services/`

Also to disable airflow from loading example dags, toggle `load_examples` to `False` in `airflow.cfg`. 

### Setup user

```shell script
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```
 
 ### Run Web service
 
 ```shell script
export DATA_SERVICES_LOGS=<PATH_TO_DATASERVICES>/logs
export DATA_SERVICES_STORAGE=<PATH_TO_DATASERVICES>/data
airflow webserver --port 8081
```

### Run Scheduler


```shell script
export DATA_SERVICES_LOGS=<PATH_TO_DATASERVICES>/logs
export DATA_SERVICES_STORAGE=<PATH_TO_DATASERVICES>/data
airflow scheduler
```
