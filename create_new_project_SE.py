from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'vedad',
    'depends_on_past': False,
    'start_date': datetime(2017,1,1),
    'email': ['vedad@socialexplorer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016,1,1)
}

dag = DAG('process_project_data', default_args=default_args)

step1 = BashOperator(task_id='check_prerequisites',
                     bash_command='echo "this will check if database is available"',
                     retries=3,
                     dag=dag)

step2 = BashOperator(task_id='load_data_into_database',
                     bash_command='ls /',
                     dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        ech0 "{{ macros.ds_add(ds, 7) }}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

step3 = BashOperator(
    task_id='create_metadata_file',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

step2.set_downstream(step1)
step3.set_downstream(step2)