
from airflow import DAG

from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator


default_args = {'start_date':'2021-01-01','owner':'airflow'}

with DAG(
    dag_id = 'parllel_1_job',
    default_args = default_args,
    schedule_interval = '@daily',
    dagrun_timeout = timedelta(minutes=1),
    tags=['dev'],
    catchup=False    
) as dag_new:
    
    task1 = BashOperator(
        task_id ='task1',
        bash_command = 'echo bala1 ; sleep 3'
    )

    task2 = BashOperator(
        task_id ='task2',
        bash_command = 'echo bala2 ; sleep 3'
    )
    
    task3 = BashOperator(
        task_id ='task3',
        bash_command = 'echo bala3 ; sleep 3'
    )
    
    task4 = BashOperator(
        task_id ='task4',
        bash_command = 'echo bala4 ; sleep 3'
    )
    
    task1 >> task2
    task2 >> task4
    task1 >> task3 >> task4

if __name__ == '__main__':
    dag_new.cli()
