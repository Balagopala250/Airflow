from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

default_args = {'start_date':'2021-01-01','owner':'airflow'}

with DAG(dag_id='trigger_rules_1',
    schedule_interval = '@daily',
    catchup = False,
    default_args = default_args
) as dags_1:

    task1 = BashOperator(
        task_id ='task1',
        bash_command = ' exit 0',
        do_xcom_push = False
    )
    task2 = BashOperator(
        task_id ='task2',
        bash_command = ' exit 1',
        do_xcom_push = False
    )

    task3 = BashOperator(
        task_id ='task3',
        bash_command = ' exit 0',
        do_xcom_push = False,
        trigger_rule = 'all_success'
    )

[task1,task2] >> task3

if __name__ == '__main__' :
    dags_1.cli()
