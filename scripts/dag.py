from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from extract import run_extract
from time import strftime
from transform import transformation
from load_db import db_load
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(2),
    'email': 'your-email',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

my_dag = DAG(
    'property_etl_dag',
    default_args=default_args,
    description='an ETL that extracts data from website, saves it to S3, does the cleaning, and saves it back to S3 and PostgreSQL',
    schedule_interval='0 8 * * *' 
)

run_extract_data = PythonOperator(
    task_id = 'data_extract',
    python_callable=run_extract,
    dag= my_dag,
)

s3_create_object_raw_data = S3CreateObjectOperator(
    s3_bucket='properties-etl',
    s3_key = 'raw_data/to_process/raw_properties_{}.csv'.format(pendulum.now('Europe/Prague').strftime("%Y_%m_%d_%H%M%S")),
    data = '{{ ti.xcom_pull(task_ids="data_extract") }}',
    task_id='create_s3_object_of_raw_data_previously_scraped',
    dag=my_dag
)

s3_sensor_for_new_raw = S3KeySensor(
    task_id='s3_sensor_for_new_raw_data_file',
    bucket_name='properties-etl',
    bucket_key='raw_data/to_process/raw_properties_*',
    wildcard_match=True,
    timeout=60*30,  # Timeout after 30 min
    poke_interval=60,  # Check every minute
    dag=my_dag,
)

transform_data = PythonOperator(
    task_id='transform_raw_data_from_s3',
    python_callable=transformation,
    dag=my_dag,
)

s3_sensor_for_db_load = S3KeySensor(
    task_id='s3_sensor_for_db_load',
    bucket_name='properties-etl',
    bucket_key='transformed_data/to_process/transformed_*',
    wildcard_match=True,
    timeout=60*60,  # Timeout after 1 hour
    poke_interval=60,  # Check every minute
    dag=my_dag,
)

load_data_to_db = PythonOperator(
    task_id='load_data_to_db',
    python_callable=db_load,
    dag=my_dag,
)


run_extract_data >> s3_create_object_raw_data >> s3_sensor_for_new_raw >> transform_data >> s3_sensor_for_db_load >> load_data_to_db
