from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,

}
template_path = "{}/sql".format(os.path.split(os.path.dirname(__file__))[0])

with DAG(
    'universities',
    default_args=default_args,
    description='extract universities from api and load to postgresql',   
    start_date=datetime(year=2024, month=5, day=12),
    schedule=timedelta(days=1, seconds=10800),
    catchup=False,
    template_searchpath=template_path,

) as dag:

    t1_ingestion = BashOperator(
        task_id='get_api_data_to_csv',
        bash_command='python3 $AIRFLOW_HOME/dags/ingestion.py',
    )
    
    with TaskGroup(group_id="loading") as extraction_group:

        t2_create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id="postgres_default",
            sql="/create_table.sql"
        )

        t3_load = BashOperator(
            task_id="load_data_to_db",
            bash_command="python3 $AIRFLOW_HOME/dags/load.py",
        )

    t2_create_table >> t3_load

t1_ingestion >> extraction_group