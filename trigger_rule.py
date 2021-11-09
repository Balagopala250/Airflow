'''


task a --> task c 
task b --> task c

all_success --> all taks are sccess then task c 
all_failed  --> if any one is succeswed the ntaks c skipped
all_done  --> 
one_success --> 
one_failed --> 
none_failed --> 
none_failed_or_skipped --> 
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    #return accuracy
    ti.xcom_push(key='p_xcom',value = accuracy)
def _choose_best_model(ti):
    print('choose best model')
    x = ti.xcom_pull(key='p_xcom',task_ids=['processing_tasks.training_model_a','processing_tasks.training_model_b','processing_tasks.training_model_c'])
    print(f'xcom values are :{x}')
    #return 'd_accurate'
    for x in x:
        if x > 2:
            return 'd_accurate'
        return 'd_inaccurate'
with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push = False
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = BranchPythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model
    )

    d_accurate = DummyOperator(task_id = 'd_accurate')

    d_inaccurate = DummyOperator(task_id = 'd_inaccurate')

    storing = DummyOperator(task_id = 'storing',trigger_rule ='one_failed' )

    downloading_data >> processing_tasks >> choose_model
    choose_model >>  [d_accurate,d_inaccurate] >> storing
