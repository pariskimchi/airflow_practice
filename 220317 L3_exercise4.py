'''
Data Quality Check 
1. Set an SLA on bikeshare traffic calculation operator 
2. add data verification step after the load step from s3 to redshift 
3. add data verification step after calculation of output table

'''


import datetime 
import logging 

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.operators.python_operator import PythonOperator 

import sql_statements 

def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    execution_date =kwargs["execution_date"]
    
    sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
        credentials.access_key, 
        credentials.secret_key, 
        year = execution_date.year, 
        month = execution_Date.month,
    )
    redshift_hook.run(sql_stmt)

# checking data quality by getting number of row from table
def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

    # log data function 
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed.{table} returned no results")
    num_records = records[0][0]

    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 rows")
    
    # log data 
    logging.info(f"Data quality on table {table} check passed with {record[0][0]} records")


dag = DAG(
    'lesson2.exercise4',
    start_date = datetime.datetime(2018, 1,1, 0,0,0,0)
    end_date = datetime.datetime(2018,12,1,0,0,0,0)
    schedule_interval = '@monthly',
    max_active_runs = 1
)

# steps 

create_trips_table = PostgresOperator(
    task_id= "create_trips_table",
    dag=dag, 
    postgres_conn_id = "redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_trips_task = PythonOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag, 
    python_callable = load_trip_data_to_redshift,
    provide_context=True,
)

# data quality checking operator step 
check_trips = PythonOpertor(
    task_id="check_trips_data",
    dag=dag, 
    python_callable=check_greater_than_zero, 
    provide_context=True,
    params = {
        'table':'trips',
    }
)

# set task dependencies 

create_trips_table >> copy_trips_task 
copy_trips_task >> check_trips