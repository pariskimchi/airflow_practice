'''
Templates?

    => allow substituting information 
    during a DAG run 
    => provide added flexibility when defining task 
    => using Jinja


'''

# Non-templated BashOperator Example 

t1 = BashOperator(
    task_id='first_task',
    bash_command='echo "Reading file1.txt"',
    dag=dag
)

# templated BashOperator example
templated_command = """
    echo "Reading {{params.filename}}"
"""

t1 = BashOperator(
    task_id='template_task',
    bash_command=templated_command, 
    params = {
        'filename':'file1.txt',
        
    },
    dag=example_dag
)

t2 = BashOperator(
    task_id='template_task2',
    bash_command = templated_command, 
    params = {
        'filename':'file2.txt',
    },
    dag=example_dag
)


# exercise 

from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator 
from datetime import datetime 

default_args = {
    'start_date':datetime(2020,4,15),
}

cleandata_dag = DAG(
    'cleandata',
    default_args = default_args,
    schedule_interval='@daily',
)

# Create a templated command to execute 
# 'bash clenadata.sh datestring'

templated_command = """
    bash cleandata.sh {{params.filename}} 
"""

clean_task = BashOperator(
    task_id='cleandata_task',
    bash_command = templated_command, 
    params = {
        'filename':'salesdata.txt',
    }
)

# more advanced template 

templated_command = """
{% for filename in params.filenames %}
    echo "Reading {{filename}}"
{% endfor %}
"""
t1 = BashOperator(
    task_id="template_task",
    bash_command=templated_command, 
    params = {
        'filenames':['file1.txt','file2.txt']
    },
    dag=example_dag
)

# exercise 

from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator 
from datetime import datetime 

file_list = [f'file{x}.txt' for x in range(30)]

default_args = {
    'start_date':datetime(2020,4,15),
}

# dag 
cleandata_dag = DAG(
    'cleandata',
    default_args = default_args, 
    schedule_interval='@daily'
)

# template command 
templated_command = """
{%  for filename in params.filenames %}
    bash cleandata.sh {{ ds_nodash}} {{filename}};
{% endfor %}
"""

# task 
clean_task = BashOperator(
    task_id="cleandata_task",
    bash_command = templated_command, 
    params = {
        'filenames':file_list
    },
    dag=cleandata_dag
)

# exercise 

from airflow.models import DAG 
from airflow.operators.email_operator import EmailOperator 
from datetime import datetime 

# create the String to html content 
html_email_str = """
Date: {{ ds }}
Username: {{ params.username }}
"""

# DAG 
email_dag = DAG(
    'template_email_test',
    default_args = {
        'start_date': datetime(2020,4,15)
    },
    schedule_interval = '@weekly'
)

# task 
email_task = EmailOperator(
    task_id="email_task",
    to='testuser@datacamp.com',
    subject="{{ macros.uuid.uuid4() }}",
    html_content = html_email_str,
    params = {
        'username':'testemailuser'
    },
    dag=email_dag
)