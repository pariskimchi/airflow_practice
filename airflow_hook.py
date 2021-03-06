from airflow import DAG 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator 

def load():
    #create a PostgresHook option using the 'demo' connection 
    db_hook = PostgresHook('demo')
    df = db_hook.get_pandas_df('SELECT * FROM rides')
    print(f'Successfully used PostgresHook to return {len(df)} records')

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag
)

'''
PostgresHook: work with Redshift
'''