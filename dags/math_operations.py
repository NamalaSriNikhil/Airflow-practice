from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_number(**context):
    context['ti'].xcom_push(key='current_value',value=10)
    print("Starting number is 10")

def Add_number(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_ids='start_number')
    current_value+=5
    print("Current number is ",current_value)
    context['ti'].xcom_push(key='current_value',value=current_value)

def Multiply_number(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_ids='Add_number')
    current_value*=2
    print("current number is ",current_value)
    context['ti'].xcom_push(key='current_value',value=current_value)

def Subtract_number(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_ids='Multiply_number')
    current_value-=3
    print("current number is ",current_value)
    context['ti'].xcom_push(key='current_value',value=current_value)

def Square_number(**context):
    current_value=context['ti'].xcom_pull(key='current_value',task_ids='Subtract_number')
    current_value=current_value*current_value
    print("current number is ",current_value)
    context['ti'].xcom_push(key='current_value',value=current_value)

with DAG(
    'math_operations',
    start_date=datetime(2025,7,1),
    schedule='@weekly'
) as dag:
    
    start=PythonOperator(task_id="start_number",python_callable=start_number)
    Add=PythonOperator(task_id="Add_number",python_callable=Add_number)
    Multiply=PythonOperator(task_id="Multiply_number",python_callable=Multiply_number)
    Subtract=PythonOperator(task_id="Subtract_number",python_callable=Subtract_number)
    Square=PythonOperator(task_id="Square_number",python_callable=Square_number)

    start >> Add >> Multiply >> Subtract >> Square

