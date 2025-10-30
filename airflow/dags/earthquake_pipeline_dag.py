
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'bensha',
    'start_date': datetime(2025, 10, 30),
    'retries': 3,
}

with DAG(
    'earthquake_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='../venv/bin/python ../producer_usgs_.py',
    )

    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='../venv/bin/python ../consumer_bq.py',
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='../venv/bin/dbt run',
        cwd='../usgs_dbt_project',
    )

    run_producer >> run_consumer >> run_dbt
