from cgitb import enable
import os
from datetime import datetime
from distutils.cmd import Command
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres  import PostgresHook
from airflow.providers.postgres.operators.postgres  import PostgresOperator
import psycopg2
from sqlalchemy import create_engine
from download_titanic_dataset import download_titanic_dataset
from pivot_dataset import pivot_dataset
from mean_fare_per_class import mean_fare_per_class


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

def download_titanic_dataset(ti):
    
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)      
    out = df.to_json(orient='records')
  
    ti.xcom_push(key='key', value=out)   

def pivot_dataset():
    titanic_data = ti.xcom_pull(key='key')
    titanic_df = pd.read_json(titanic_data)
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
    return df.to_sql(f'{Variable.get("var")}', engine)

def mean_fare_per_class():
    titanic_data = ti.xcom_pull(key='key')
    titanic_mean_df = pd.read_json(titanic_data)
    df2 = titanic_mean_df.set_index('Pclass')[['Fare']].stack().mean(level=0)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@localhost:5432/airflow')
    return df2.to_sql(f'{Variable.get("var2")}', engine)



dag = DAG('new_version_titanic', description='pivot_titanic',
          schedule_interval='@once',
          start_date=datetime(2017, 7, 18),
          catchup=False)


first_task = BashOperator(
    task_id='first_task',
    bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)


create_titanic_dataset = PythonOperator(
    task_id='download_titanic_dataset',
    python_callable=download_titanic_dataset,
    dag=dag,
    provide_context=True
  
)


pivot_titanic_dataset = PythonOperator(
    task_id='pivot_dataset',
    python_callable=pivot_dataset,
    dag=dag,
)

mean_fares_titanic_dataset = PythonOperator(
    task_id='mean_fare_per_class',
    python_callable=mean_fare_per_class,
    dag=dag,
)

last_task = BashOperator(
    task_id='last_task',
    bash_command= 'echo "Pipeline finished! Execution date is" $(date +"%Y-%m-%d")',
    dag=dag,
)





# Порядок выполнения тасок
first_task >> create_titanic_dataset >> [pivot_titanic_dataset, mean_fares_titanic_dataset] >> last_task
