
'''
Running DAGs + TAsks 

`airflow run <dag_id> <task_id> <date>`

'''

from airflow.models import DAG 
from airflow.contrib.sensors.file_sensor import FileSensor 

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime


# function to process data 
def process_data(**context):
  file = open('/home/repl/workspace/processed_data.tmp', 'w')
  file.write(f'Data processed on {date.today()}')
  file.close()

# DAG 
dag = DAG(
    dag_id='etl_update',
    default_args:{
        'start_date': datetime(2020,4,1)
    }
)

# sensor 
sensor = FileSensor(task_id='sense_file', 
                    filepath='/home/repl/workspace/startprocess.txt',
                    poke_interval=5,
                    timeout=15,
                    dag=dag)

bash_task = BashOperator(task_id='cleanup_tempfiles', 
                         bash_command='rm -f /home/repl/*.tmp',
                         dag=dag)

python_task = PythonOperator(task_id='run_processing', 
                             python_callable=process_data,
                             dag=dag)

sensor >> bash_task >> python_task


# exercise 2 

from airflow.models import DAG 
from airflow.contrib.sensors.file_sensor import FileSensor 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator 
from dags.process import process_data 
from datetime import timedelta, datetime 

default_args = {
    'start_date':datetime(2019,1,1),
    'sla':timedelta(minutes=90)
}

# DAG 
dag = DAG(
    dag_id='etl_update',
    default_args=default_args
)

# sensor 
sensor = FileSensor(task_id='sense_file', 
                    filepath='/home/repl/workspace/startprocess.txt',
                    poke_interval=45,
                    dag=dag)
bash_task = BashOperator(task_id='cleanup_tempfiles', 
                         bash_command='rm -f /home/repl/*.tmp',
                         dag=dag)

python_task = PythonOperator(task_id='run_processing', 
                             python_callable=process_data,
                             provide_context=True,
                             dag=dag)

sensor >> bash_task >> python_task
