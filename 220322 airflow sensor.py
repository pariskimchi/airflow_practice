'''
sensor?
=> an operator that waits for a certain condition to be true 
    - creation of a file 
    - Upload of a database record 
    - Certain response from a web request

Sensor Details

mode = 'poke' => run repeatedly
mode = 'reschedule' => give up task slot and try again later

poke_interval => how often to wait between checks 

timeout => how long to wait before failing task 
'''

# File sensor 

from airflow.contrib.sensors.file_sensor import FileSensor 

file_sensor_task = FileSensor(
    task_id = 'file_sense',
    filepath = 'salesdata.csv',
    poke_interval = 300, 
    dag = sales_report_dag
)
init_sales_cleanup >> file_sensor_task >> generate_report

'''
Other Sensors
    - ExternalTaskSensor: wait for a task in another Dag to complete 
    - HttpSensor : Request a web URL and check for content 
    - SqlSensor: Runs a SQL query to check for content 


'''

'''
Executor?
    => Executor run tasks

    - SequentialExecutor:
        => default Airflow executor 
        => runs one task at a time 
        => useful for debugging 
        => 
    - LocalExecutor :
        => runs on a single system 
        => treat tasks as processes 
        => parallelism defined by the user 
        =>

    - CeleryExecutor:
        => uses a Celery backened as task manager 
        => mulitple worker system 
        => diffult to setup and configure 

'''

# determine executor 
# via airflow.cfg
def_executor_cmd = """
    cat airflow/airflow.cfg | grep "executor ="
    executor = SequentialExecutor
"""
# or
def_executor_cmd2 = " airflow list_dags"