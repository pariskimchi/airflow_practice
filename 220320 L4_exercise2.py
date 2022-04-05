

import datetime 
import logging 

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator 
from airflow.operators.python_operator import PythonOperator 



def load_and_analyze(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")

    # find all trips where the rider was under 18
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """)

    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders 
        ORDER BY birthyear DESC LIMIT 1
    """)

    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

# modified task function 
def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


# youngest 
def log_youngest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")



dag = DAG(
    "lesson3.exercise2",
    start_date = datetime.datetime.utcnow()
)

create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag, 
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag, 
    python_callable=log_oldest
)

# youngest 

create_youngest_task = PostgresOperator(
    task_id="create_youngest",
    dag=dag, 
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,

    postgres_conn_id="redshift"
)

# print log task 
log_youngest_task = PythonOperator(
    task_id="log_youngest",
    dag=dag,
    python_callable=log_youngest
)




create_oldest_task >> log_oldest_task
create_youngest_task >> log_youngest_task