

import datetime 

from airflow import DAG 

from airflow.operators import (
    FactsCalculatorOperator, 
    HasRowOperator, 
    S3ToRedshiftOperator
)

# 1. Loads Trip data from S3 to redshift 
# 2. perform a data quality check on the trips table in redshift 
# 3. Uses the FactCalculatorOperator to create a Fact table in redshif t


dag = DAG(
    "lesson3.exercise4",
    start_date = datetime.datetime.utcnow()
)

# from s3 to redshift 
copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag, 
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

# data quality check on the trips table 
check_trips = HasRowOperator(
    task_id="check_trips_data",
    dag=dag, 
    redshift_conn_id="redshift",
    table="trips"
)

# using FactCalculatorOperator 
# to create a Facts table in redshift 

calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts_trips",
    dag=dag, 
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trips_facts",
    fact_column="tripduration",
    groupby_column="bikeid"
)

# task ordering 
copy_trips_task >> check_trips
check_trips >> calculate_facts