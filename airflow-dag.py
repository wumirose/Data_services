from Common.load_manager import SourceDataLoadManager

import os

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

from airflow.models import DAG


default_args = {
    'owner': 'RENCI-DATA-SERVICES',
    'start_date': days_ago(1)
}


def task_wrapper(python_callable, **kwargs):
    """
    Overrides configuration with config from airflow.
    :param python_callable:
    :param kwargs:
    :return:
    """
    # get dag config provided
    dag_run = kwargs.get('dag_run')
    if dag_run:
        dag_conf = dag_run.conf
        # remove this since to send every other argument to the python callable.
        del kwargs['dag_run']
    # overrides values
    return python_callable(kwargs['source_id'])


def get_executor_config(data_path='/opt/data'):
    """ Get an executor configuration.
    :param annotations: Annotations to attach to the executor.
    :returns: Returns a KubernetesExecutor if K8s is configured and None otherwise.
    """
    # based on environment set on scheduler pod, make secrets for worker pod
    # this ensures passwords don't leak as pod templates.
    secrets_map = []
    secrets = []
    for secret in secrets_map:
        secret_name = os.environ.get(secret["secret_name_ref"], False)
        secret_key_name = os.environ.get(secret["secret_key_ref"], False)
        if secret_name and secret_key_name:
            secrets.append({
                "name": secret["env_var_name"],
                "valueFrom": {
                    "secretKeyRef": {
                       "name": secret_name,
                       "key": secret_key_name
                    }
                }})

    k8s_executor_config = {
        "KubernetesExecutor": {
            "envs": secrets,
            "volumes": [
                {
                    "name": "dataservices-data",
                    "persistentVolumeClaim": {
                        "claimName": "dataservices-data-pvc"
                    }
                }
            ],
            "volume_mounts": [
                {
                    "mountPath": data_path,
                    "name": "dataservices-data",
                    "subpath": "data"
                },{
                    "mountPath": "/opt/airflow/logs",
                    "name": "dataservices-data",
                    "subpath": "task-logs"
                }
            ]
        }
    }
    return k8s_executor_config


def create_python_task (dag, name, a_callable, func_kwargs=None):
    """ Create a python task.
    :param func_kwargs: additional arguments for callable.
    :param dag: dag to add task to.
    :param name: The name of the task.
    :param a_callable: The code to run in this task.
    """
    op_kwargs = {
            "python_callable": a_callable
        }
    if func_kwargs is None:
        func_kwargs = dict()
    op_kwargs.update(func_kwargs)
    return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs=op_kwargs,
        executor_config=get_executor_config(),
        dag=dag,
        provide_context=True
    )


def create_dag_from_source(dag_id, source, sdl):
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=None
    ) as dag:
        # @todo ./data needs to be a more abs dir, when moving to k8s executors but for local it should be fine.
        # @todo maybe make a file-browser side car container

        # check_for_update should be the first step - branching to either read_source if True or exiting if False
        # check_for_update = BranchPythonOperator(task_id=f'check_for_update-{source}', python_callable=sdl.check_if_source_needs_update
        read_source = create_python_task(dag, f'grab_from_source-{source}', a_callable=sdl.update_source, func_kwargs={'source_id': source})
        normalize_task = create_python_task(dag, f'normalize_source-{source}', a_callable=sdl.normalize_source, func_kwargs={'source_id': source})
        supplementation = create_python_task(dag, f'supplement-source-{source}', a_callable=sdl.supplement_source, func_kwargs={'source_id': source})
        read_source >> normalize_task >> supplementation
    return dag


sdl = SourceDataLoadManager()
for source_id in sdl.source_list:
    dag_id = f"{source_id}_DS_DAG"
    globals()[dag_id] = create_dag_from_source(dag_id=dag_id, source=source_id, sdl=sdl)
