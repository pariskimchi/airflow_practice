'''
Context and Templating

로그 데이터 출력

*args,/**kwargs 이용
'''

import datetime 
import logging 

from airflow import DAG
from airflow.models import Variable 
from airflow.operators.python_operator import PythonOperator 
from airflow.hooks.S3_hook import S3Hook

def log_details(*args, **kwargs):

    # Extract ds, run_id, prev_ds, next_ds from kwargs and log them 

    ds = kwargs['ds'] # kwargs[]
    run_id = kwargs['run_id'] # kwargs[]
    previous_ds = kwargs.get('prev_ds') # kwargs.get('')
    next_ds = kwargs.get('next_ds') # kwargs.get('')

    logging.info(f"Execution date is {ds}")
    logging.info(f"My run id is {run_id}")

    if prev_ds:
        logging.info(f"My previous run was on {prev_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    'lesson1.exercise5',
    schedule_interval = "@daily",
    start_date = datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context = True,
    dag=dag
)