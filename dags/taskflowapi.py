from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='math_sequence_dagwith_airflow',
    start_date=datetime(2025,7,1),
    schedule='@once',
    catchup=False,
) as dag:
    
    @task
    def start_number():
        initial_number=10
        print("current number is:",initial_number)
        return initial_number
    
    @task
    def add_number(number):
        current_number=number+5
        print("current number is:",current_number)
        return current_number
    
    @task
    def multiply_number(number):
        current_number=number*2
        print("current number is:",current_number)
        return current_number
    
    @task
    def subtract_number(number):
        current_number=number-3
        print("current number is:",current_number)
        return current_number
    
    @task
    def square_number(number):
        current_number=number*number
        print("current number is:",current_number)
        return current_number
    
    initial_value=start_number()
    current_value=add_number(initial_value)
    current_value=multiply_number(current_value)
    current_value=subtract_number(current_value)
    current_value=square_number(current_value)