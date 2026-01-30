from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# 1. Define the Project Root Path dynamically
# This ensures it works no matter where Airflow is running from
# We assume this file is in stock-market-pipeline/airflow/dags/
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))

# 2. Default Arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 3. Define the DAG
with DAG(
    'stock_market_batch_pipeline',
    default_args=default_args,
    description='Daily ingestion of company metadata and dbt transformation',
    schedule_interval='@daily', # Runs once every midnight
    start_date=datetime(2023, 1, 1),
    catchup=False, # Don't run for past dates
    tags=['stock', 'etl'],
) as dag:

    # Task 1: Fetch Company Metadata (Python Script)
    fetch_metadata = BashOperator(
        task_id='fetch_company_metadata',
        # Change directory to project root, then run script
        bash_command=f'cd {project_root} && python scripts/fetch_company_meta.py'
    )

    # Task 2: Run dbt Models
    dbt_run = BashOperator(
        task_id='dbt_run',
        # Change directory to dbt project, then run dbt
        bash_command=f'cd {project_root}/dbt_project && dbt run'
    )

    # Task 3: Run dbt Tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {project_root}/dbt_project && dbt test'
    )

    # 4. Define Dependency Chain
    fetch_metadata >> dbt_run >> dbt_test