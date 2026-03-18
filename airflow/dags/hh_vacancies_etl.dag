from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'hh_vacancies_collect',
    start_date=datetime(2026, 3, 1),
    schedule_interval='0 2 * * *',  # каждый день в 02:00
    catchup=False
)

collect = BashOperator(
    task_id='collect_vacancies',
    bash_command='cd /path/to/project && .venv/bin/python -m scripts.collector_smart',
    dag=dag
)