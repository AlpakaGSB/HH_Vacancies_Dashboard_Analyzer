from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    dag_id='hh_vacancies_daily_collect',
    default_args=default_args,
    description='Ежедневный сбор вакансий с hh.ru (публичный API)',
    schedule_interval='0 2 * * *',          # каждый день в 02:00
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['hh', 'vacancies', 'etl'],
)

collect_task = BashOperator(
    task_id='run_collector_smart',
    bash_command=(
        'cd /home/insid/PycharmProjects/HH_Vacancies_Dashboard_Analyzer && '
        '.venv/bin/python -m scripts.collector_auto'
    ),
    dag=dag,
)