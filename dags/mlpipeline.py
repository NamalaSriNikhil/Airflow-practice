from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def preprocess_model():
    print("Preprocessing Model...")

def train_model():
    print("Training model...")

def evaluate_model():
    print("Evaluating Model...")

with DAG(
    'ml_pipeline',
    start_date=datetime(2025,1,1),
    schedule='@weekly'
) as dag:
    
    preprocess=PythonOperator(task_id="preprocess_model",python_callable=preprocess_model)
    train=PythonOperator(task_id="train_model",python_callable=train_model)
    evaluate=PythonOperator(task_id="evaluate_model",python_callable=evaluate_model)

    preprocess >> train >> evaluate