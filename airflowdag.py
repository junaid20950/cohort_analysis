from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
from pipeline_script import extract_data, transform_data, upload_to_gcs, load_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'cohort_retention_pipeline',
    default_args=default_args,
    description='Cohort retention analysis pipeline',
    schedule_interval='0 */8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data)
    upload_task = PythonOperator(task_id='upload_to_gcs', python_callable=upload_to_gcs)
    load_task = PythonOperator(task_id='load_to_bigquery', python_callable=load_to_bigquery)

    extract_task >> transform_task >> upload_task >> load_task
