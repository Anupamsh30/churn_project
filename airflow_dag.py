# airflow_dag.py
import sys
sys.path.append('/Users/anupam.sharma/Documents/personal/customer_churn_pipeline/')
import os
print(os.getcwd())

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

from scripts.data_fetch import fetch_data
from scripts.data_validation import validate_data
from scripts.data_cleaning import clean_data
from scripts.data_store import store_partitioned_data
from scripts.feature_engineering import create_features
from scripts.feature_engineering import create_features
from scripts.feature_store import store_features_in_sql
from scripts.model_training import train_model

logging.info(os.getcwd())

# Set up logging to a custom file
log_file_path = os.path.join("./logs/error_logs.log")
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def fetch_data_task():
    logging.info("Fetching data...")
    return fetch_data()

def validate_data_task(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_task')
    data = data[0]
    validate_data(data)
    return data

def clean_data_task(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='validate_data_task')
    ti.xcom_push(key="data", value = "./airflow/test.csv")
    cleaned_data = clean_data(data)
    return cleaned_data

def store_raw_data_task(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_data_task')
    store_partitioned_data(raw_data[0], 'data/raw/', 'customerID', 'csv')
    store_partitioned_data(raw_data[1], 'data/raw/', 'customerID', 'sql')

def feature_engineering_task(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='clean_data_task')
    features = create_features(cleaned_data)
    return features

def store_processed_data_task(**kwargs):
    ti = kwargs['ti']
    features = ti.xcom_pull(task_ids='feature_engineering_task')
    store_partitioned_data(features, 'data/processed', 'customerID', 'sql')

def store_features_task(**kwargs):
    ti = kwargs['ti']
    features = ti.xcom_pull(task_ids='feature_engineering_task')
    store_features_in_sql(features, 'data/features.db')

def train_model_task(**kwargs):
    ti = kwargs['ti']
    features = ti.xcom_pull(task_ids='feature_engineering_task')
    model = train_model(features.drop('target', axis=1), features['target'])
    logging.info("Model training complete.")

# Define Airflow DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'customer_churn_pipeline',
    default_args=default_args,
    description='End-to-end customer churn pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

fetch_data_op = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data_task,
    dag=dag,
)

validate_data_op = PythonOperator(
    task_id='validate_data_task',
    python_callable=validate_data_task,
    provide_context=True,
    dag=dag,
)

clean_data_op = PythonOperator(
    task_id='clean_data_task',
    python_callable=clean_data_task,
    provide_context=True,
    dag=dag,
)

store_raw_data_op = PythonOperator(
    task_id='store_raw_data_task',
    python_callable=store_raw_data_task,
    provide_context=True,
    dag=dag,
)


feature_engineering_op = PythonOperator(
    task_id='feature_engineering_task',
    python_callable=feature_engineering_task,
    provide_context=True,
    dag=dag,
)

store_processed_data_op = PythonOperator(
    task_id='store_processed_data_task',
    python_callable=store_processed_data_task,
    provide_context=True,
    dag=dag,
)

store_features_op = PythonOperator(
    task_id='store_features_task',
    python_callable=store_features_task,
    provide_context=True,
    dag=dag,
)

train_model_op = PythonOperator(
    task_id='train_model_task',
    python_callable=train_model_task,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
fetch_data_op >> [store_raw_data_op, validate_data_op]
validate_data_op >> clean_data_op >> feature_engineering_op
feature_engineering_op >> [store_processed_data_op, store_features_op, train_model_op]
