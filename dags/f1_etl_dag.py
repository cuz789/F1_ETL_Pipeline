from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Teja',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='f1_etl_pipeline',
    description='Full F1 ETL DAG with all stages',
    default_args=default_args,
    start_date=datetime(2025, 8, 7),
    schedule_interval='@once',
    catchup=False
) as dag:

    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/scripts/Extract.py'
    )

    transform_drivers = BashOperator(
        task_id='transform_drivers',
        bash_command='python /opt/airflow/scripts/drivers_transform.py'
    )

    transform_meetings = BashOperator(
        task_id='transform_meetings',
        bash_command='python /opt/airflow/scripts/meetings_transform.py'
    )

    transform_sessionresults = BashOperator(
        task_id='transform_sessionresults',
        bash_command='python /opt/airflow/scripts/sessionresults_transform.py'
    )

    transform_sessions = BashOperator(
        task_id='transform_sessions',
        bash_command='python /opt/airflow/scripts/sessions_transform.py'
    )

    transform_startinggrid = BashOperator(
        task_id='transform_startinggrid',
        bash_command='python /opt/airflow/scripts/startinggrid_transform.py'
    )

    load_data = BashOperator(
        task_id='load_data',
        bash_command='python /opt/airflow/scripts/load.py'
    )

    # Define dependencies
    extract_data >> [transform_drivers, transform_meetings, transform_sessionresults, transform_sessions, transform_startinggrid]
    [transform_drivers, transform_meetings, transform_sessionresults, transform_sessions, transform_startinggrid] >> load_data