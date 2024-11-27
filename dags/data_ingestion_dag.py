from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys


sys.path.append('/opt/airflow/scripts')
from data_ingestion import ingest_streams_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'streams_data_ingestion',
    default_args=default_args,
    description='Ingest streams data to Iceberg',
    schedule_interval='0 0 * * 1',
    catchup=False,
    max_active_runs=1
)

start = EmptyOperator(task_id='start', dag=dag)

ingest_task = PythonOperator(
    task_id='ingest_streams_data',
    python_callable=ingest_streams_data,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> ingest_task >> end