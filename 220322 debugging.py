'''

# DAG won't load 
    - DAG not in web UI
    - DAG not in `airflow list_dags`

    # solution
    => `head airflow/airflow.cfg`
    => verify DAG file is in correct folder
    => determine the DAGs folder via `airflow.cfg` 

# syntax error 

    # solution
    => `airflow list_dags`
    => `python3 dagfile.py`


# SLA?
    => Service Level Agreement 
    => amount of time  before run dag 

'''
# Using 'sla' argument on the task 

task1 = BashOperator(
    task_id="sla_task",
    bash_command = 'runcode.sh',
    sla=timedelta(seconds=30),
    dag=dag
)

# on the defaults_args dictionary 
default_args = {
    'sla': timedelta(minutes=20), 
    'start_date':datetime(2020,2,20)
}

# email task 

email_report = EmailOperator(
    task_id="email_report",
    to='airflow@datacamp.com',
    subject='Airflow Monthly Report',
    html_content = """
    Attached is your monthly workflow report - 
    please refer to it for more detail
    """
    files = ['monthly_report.pdf'],
    dag=report_dag
)


