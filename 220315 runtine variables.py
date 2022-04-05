'''
Building a Data Pipeline in airflow
'''


from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 

def hello_date(*args, **kwargs):
    print(f"Hello {kwargs['execution_date']}")

divvy_dag = DAG()
task  = PythonOperator(
    task_id="hello_date",
    python_callable=hello_date,
    provide_context = True,
    dag=divvy_dag
)