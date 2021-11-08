'''
Complex data pipe lines 


grouping tasks in clearner 
run the tasks based on the receing files 
conditional runs ...

SubDags --> to group the subtaks ..


Negatives(not recommened in production) : 
Deadlocks 
Complexity
own executor : sequential excutor -->

'''

from airflow.operators.bash import BashOperator
from airflow import DAG

def sub_process_exp(parent_dag_id,child_dag_id,default_args):
    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}', default_args=default_args) as dag:
        task2 = BashOperator(
            task_id ='task2',
            bash_command = 'echo bala2 ; sleep 3'
        )
        
        task3 = BashOperator(
            task_id ='task3',
            bash_command = 'echo bala3 ; sleep 3'
        )

        return dag
        
        

from airflow import DAG

from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.sub_dag_1234 import sub_process_exp

default_args = {'start_date':'2021-01-01','owner':'airflow'}

with DAG(
    dag_id = 'sub_dag_example1',
    default_args = default_args,
    schedule_interval = '@daily',
    dagrun_timeout = timedelta(minutes=3),
    tags=['dev'],
    catchup=False    
) as dag_new:
    
    task1 = BashOperator(
        task_id ='task1',
        bash_command = 'echo bala1 ; sleep 3'
    )

    process_tasks = SubDagOperator(
        task_id ='process_tasks',
        subdag = sub_process_exp('sub_dag_example1','process_tasks',default_args)
    )
    
    task4 = BashOperator(
        task_id ='task4',
        bash_command = 'echo bala4 ; sleep 3'
    )
    
    task1 >> process_tasks >> task4

if __name__ == '__main__':
    dag_new.cli()
