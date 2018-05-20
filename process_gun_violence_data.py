from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


def prepare_env():
    pass


def clean_data():
    pass


def fix_outliers():
    pass


def fix_missing_values():
    pass


def transform_for_db():
    pass


def write_to_db():
    pass


def generate_metadata():
    pass


def deploy_to_test_environment():
    pass


args = {
    'owner': 'vedad',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1),
    'email': ['vedad@socialexplorer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='process_gun_violence_data',
    default_args=args,
    schedule_interval=None
)

prepare_environment = PythonOperator(
    task_id='prepare_env',
    provide_context=True,
    python_callable=prepare_env,
    dag=dag
)

data_download = BashOperator(
    task_id='download_data',
    bash_command='kaggle datasets download -d jameslko/gun-violence-data',
    retries=3,
    dag=dag)

data_bck = BashOperator(
    task_id='data_backup',
    bash_command='zip bck.zip file1 ',
    dag=dag)

data_cleansing = PythonOperator(
    task_id='clean_data',
    provide_context=True,
    python_callable=clean_data,
    dag=dag
)

outliers_fix = PythonOperator(
    task_id='fix_outliers',
    provide_context=True,
    python_callable=fix_outliers,
    dag=dag
)

missing_values_fix = PythonOperator(
    task_id='fix_missing_values',
    provide_context=True,
    python_callable=fix_missing_values,
    dag=dag
)

db_preparation = PythonOperator(
    task_id='transform_for_db',
    provide_context=True,
    python_callable=transform_for_db,
    dag=dag
)

db_load = PythonOperator(
    task_id='write_to_db',
    provide_context=True,
    python_callable=write_to_db,
    dag=dag
)

metadata_generate = PythonOperator(
    task_id='generate_metadata',
    provide_context=True,
    python_callable=generate_metadata,
    dag=dag
)

test_deploy = PythonOperator(
    task_id='deploy_to_test_environment',
    provide_context=True,
    python_callable=deploy_to_test_environment,
    dag=dag
)

prepare_environment << \
    data_download << \
    data_bck << \
    data_cleansing << \
    outliers_fix << \
    missing_values_fix << \
    db_preparation << \
    db_load << \
    metadata_generate << \
    test_deploy

# prepare_environment.set_downstream(data_download)\
#     .set_downstream(data_cleansing)\
#     .set_downstream(outliers_fix)\
#     .set_downstream(missing_values_fix)\
#     .set_downstream(db_preparation)\
#     .set_downstream(db_load)\
#     .set_downstream(metadata_generate)\
#     .set_downstream(test_deploy)

# data_download.set_downstream(data_bck)
# data_download.set_downstream(data_cleansing)
# data_cleansing.set_downstream(outliers_fix)
# outliers_fix.set_downstream(missing_values_fix)
# missing_values_fix.set_downstream(db_preparation)
# db_preparation.set_downstream(db_load)
# db_load.set_downstream(metadata_generate)
# metadata_generate.set_downstream(test_deploy)