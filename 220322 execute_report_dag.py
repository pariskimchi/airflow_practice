from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime 

default_args = {
    'email' = ['airflowalerts@datacamp.com',
        'airflowadmin@datacamp.com'],
    'email_on_failure': True,
    'email_on_success': True
}

report_dag = DAG(
    dag_id="execute_report",
    schedule_interval= "0 0 * * *",
    default_args = default_args
)

# check if csv file uploaded using FileSensor 
precheck = FileSensor(
    task_id="check_for_datafile",
    filepath = 'salesdata_ready.csv',
    start_date = datetime(2020,2,20),
    mode='reschedule',
    dag=report_dag
)

# task to run report.sh
generate_report_task = BashOperator(
    task_id="generate_report",
    bash_command="generate_report.sh",
    start_date = datetime(2020,2,20), 
    dag=report_dag
)
 
# task dependencies 
precheck >> generate_report_task