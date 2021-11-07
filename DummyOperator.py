""" 
    This is sample programming to learn the python airflow dags with bash and dummy operators 
        with basic required details so we can start working on it in future directly 
            .. lets start and keep the marks here with important topics .. 
            Bala
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args_default = { "owner" : "airflow"
}

with DAG(
    dag_id = "Bala_t1",
    default_args = args_default,
    schedule_interval = '0 0 * * *',
    start_date = days_ago(2),
    dagrun_timeout = timedelta(minutes=60),
    tags = ["b1","b2"],
    params = {"example_key": "example_value"},
) as dag_1:
    d1 = DummyOperator(
        task_id ="Dummy_last",
    )

    b1 = BashOperator(
        task_id = "bash_command",
        bash_command = "echo 123",
    )
    
    b1 >> d1

if __name__ == "__main__":
    dag_1.cli()
