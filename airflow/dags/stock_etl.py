from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'stock_market_batch_pipeline',
    default_args=default_args,
    description='Daily ingestion and dbt transformation',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock', 'etl'],
) as dag:

    # Task 1: Fetch Company Metadata
    # In Docker, we mounted ./scripts to /opt/airflow/scripts
    fetch_metadata = BashOperator(
        task_id='fetch_company_metadata',
        bash_command='python /opt/airflow/scripts/fetch_company_meta.py'
    )

    # Task 2: Run dbt Models
    # In Docker, we mounted ./dbt_project to /opt/airflow/dbt_project
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run'
    )

    # Task 3: Run dbt Tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test'
    )

    fetch_metadata >> dbt_run >> dbt_test