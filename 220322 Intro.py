
# DAG code example 

etl_dag = DAG(
    dag_id="etl_pipeline",
    default_args = {"start_date":"2020-01-08s"}
)

# Running a workflow in Airflow 

run_command = "airflow run <dag_id> <task_id> <start_date>"

ex_run_command = "airflow run example-etl download-file 2020-01-10"


# Define a DAG 
from airflow.models import DAG 

from datetime import datetime 

default_arguments = {
    'owner':'haneul',
    'email':'haneul@email.com',
    'start_date':datetime(2020,1,20)
}

etl_dag = DAG(
    'etl_workflow',default_args=default_arguments
)

# how many dag? 
count_dag_cmd = "airflow list_dags"

# how to start the airflow UI webserver on port 9090?

port_cmd = "airflow webserver -p 9090"


# BashOperator 
# executes a given Bash command

from airflow.operators.bash_operator import BashOperator


BashOperator(
    task_id="bash_example",
    bash_command = 'echo "Example!"',
    dag=ml_dag
)

BashOperator(
    task_id='bash_script_example',
    bash_command='runcleanup.sh',
    dag=ml_dag
)

example_task = BashOperator(
    task_id="bash_ex",
    bash_command='echo 1',
    dag=dag
)
bash_task = BashOperator(
    task_id="clean_address",
    bash_command = 'cat addresses.txt | awk "NF==10" > cleaned.txt',
    dag=dag
)

# Task dependencies 
# Task Order 

task1 = BashOperator(
    task_id="first_task",
    bash_command = 'echo 1',
    dag=example_dag
)

task2 = BashOperator(
    task_id="second_task",
    bash_command='echo 2',
    dag=example_dag
)

task1 >> task2


### PythonOperator 

from airflow.operators.python_operator import PythonOperator 

def printme():
    print("This goes in the logs!")

python_task = PythonOperator(
    task_id="simple_print",
    python_callable=printme, 
    dag=example_dag
)

# op_kwargs example 

def sleep(length_of_time):
    time.sleep(length_of_time)

sleep_task = PythonOperator(
    task_id="sleep",
    python_callable=sleep,
    op_kwargs = {'length_of_time':5},
    dag=example_dag

)

# EmailOperator 

from airflow.operators.email_operator import EmailOperator 

email_task = EmailOperator(
    task_id="email_sales_report",
    to = "sales_manager@example.com",
    subject = "Automated Sales report",
    html_content = "attached is the latest sales report",
    files = "latest_sales.xlsx",
    dag=example_dag

)


# exercise 
import requests

def pull_file(URL, savepath):
    r = requests.get(URL)
    with open(savepath, 'wb') as f:
        f.write(r.content)
    # use the print method for logging 
    print(f"File pulled from {URL} and save to {savepath}")

from airflow.operators.python_operator import PythonOperator 

# Create the task 
pull_file_task = PythonOperator(
    task_id="pull_file",
    # Add the callable 
    python_callable = pull_file,
    # Define the arguments 
    op_kwargs = {
        'URL':'https://dataserver/sales.json',
        'savepath':'latestsales.json'
    },
    dag = process_sales_dag
)


# exercise 

# Import the Operator
from airflow.operators.email_operator import EmailOperator 

# Define the task 
email_manager_task = EmailOperator(
    task_id="email_manager",
    to = 'manager@datacamp.com',
    subject = 'Latest sales JSON',
    html_content = 'Attached is the latest sales JSON file as requested',
    files = 'parsedfile.json',
    dag= process_sales_dag
)

