
from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator 

import json
from pandas import json_normalize

default_args = {"owner":"airflow"}

def _python_opeator_fn(ti):
    users = ti.xcom_pull(task_ids=['Http_operator'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    
    user = users[0]['results'][0]
    
    final_user = json_normalize({
        'name' : user['name']['first'],
        'last_name' : user['name']['last'],
        'age' : user['email']
    })
    
    final_user.to_csv('/tmp/final_user.csv',index = None, header = False)
    

with DAG(
    dag_id = 'working_first_project',
    default_args = default_args,
    schedule_interval = '0 0 * * *',
    start_date = days_ago(2),
    dagrun_timeout = timedelta(seconds=30),
    tags = ["dev","testing"],
    params = {"example_key": "example_value"},   
    catchup = True
) as dag1:
    val1 = SqliteOperator(
        task_id ="create_table",
        sqlite_conn_id = 'sql_conn',
        sql = '''
            create table if not exists bala_1 ( 
            name text not null,
            last_name text not null,
            age text
            );
            '''
        )
        
    http_task = HttpSensor(
        task_id ='Http_task',
        http_conn_id = 'http_conn',
        endpoint = 'api/'
    )
    
    http_operator = SimpleHttpOperator(
        task_id = 'Http_operator',
        http_conn_id = 'http_conn',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )
        
    py_operator = PythonOperator(
        task_id = 'py_opeator',
        python_callable = _python_opeator_fn
    )
    
    bash_operator = BashOperator(
        task_id = 'bash_operator',
        bash_command = 'echo -e ".separator ","\n.import /tmp/final_user.csv bala_1" | sqlite3 /home/airflow/airflow/airflow.db'
    )
    
    val1 >> http_task >> http_operator >> py_operator >> bash_operator
    
if __name__ == '__main__':
    dag1.cli()
