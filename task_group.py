'''

'''



from airflow import DAG

from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


default_args = {'start_date':'2021-01-01','owner':'airflow'}

with DAG(
    dag_id = 'taskgroup',
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
'''
    with TaskGroup('process_task_group') as process_task_group:
       task2 = BashOperator(
           task_id ='task2',
           bash_command = 'echo bala2 ; sleep 3'
       )
       
       task3 = BashOperator(
           task_id ='task3',
           bash_command = 'echo bala3 ; sleep 3'
       )
'''

    with TaskGroup('process_task_group') as process_task_group:
       task2 = BashOperator(
           task_id ='task2',
           bash_command = 'echo bala2 ; sleep 3'
       )
       
       with TaskGroup('spark_code') as spark_code:
          task3 = BashOperator(
              task_id ='task3',
              bash_command = 'echo bala3 ; sleep 3'
          )
          
       with TaskGroup('gen_code') as gen_code:
          task3 = BashOperator(
              task_id ='task3',
              bash_command = 'echo bala3 ; sleep 3'
          )
       
    task4 = BashOperator(
        task_id ='task4',
        bash_command = 'echo bala4 ; sleep 3'
    )
    
    task1 >> process_task_group >> task4

if __name__ == '__main__':
    dag_new.cli()
