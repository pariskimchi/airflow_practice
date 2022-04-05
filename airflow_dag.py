
import datetime 
import logging 

from airflow import DAG 
from airlofw.operators.python_operator import PythonOperator 

def hello_world():
    logging.info("Hello World!")

dag = DAG(
    'lesson1.exercise1',
    start_date = datetime.datetime.now()
)

greet_task = PythonOperator(
    task_id="hello_world_task",
    python_callable=hello_world,
    dag=dag
)

# /opt/airflow/start.sh

