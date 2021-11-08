
#Celery executor 

# use below command to install the celery in the env
#pip install 'apache-airflow[celery]'
#pip install --upgrade apache-airflow-providers-celery==2.0.0 to support proper celery commands 

#we need to have queue to process the requests for that we are maining the rediss for this 

'''

sudo apt update 

sudo apt install redis-server 

sudo nano /etc/redis/redis.conf  --> modify supervised systemd


sudo systemctl restart redis.service  --> so latest changes will be reflected

sudo systemctl status redis.service --> to check the queue status 

pip install 'apache-airflow[redis]'
'''

'''
Config changes for the redis and celery executor

executor = CeleryExecutor
broker_url = redis://redis:6379/0 --> broker_url = redis://localhost:6379/0
sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@localhost/postgres


result_backend = db+postgresql://postgres:airflow@postgres/airflow --> db+postgresql://postgres:postgres@localhost/postgres

'''

'''

airflow celery flower --> to run the flower 

airflow celery worker --> to see all the workers to be ran 

including this we need to webserver and scheduler as well 
'''
