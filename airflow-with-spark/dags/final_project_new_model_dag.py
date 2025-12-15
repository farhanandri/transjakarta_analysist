import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from scripts.extract_final_project import main_extract_callable
from scripts.transform_new_model_final_project import main_transform_new_model_callable
from scripts.validate_new_model_final_project import main_validate_new_model_callable
from scripts.load_new_model_final_project import main_load_new_model_callable



default_args = {
    'owner': 'final_project',
    'start_date': dt.datetime(2025, 11, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

with DAG('tj_new_model_dag',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         catchup=False,
         ) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=main_extract_callable, 
        dag=dag,
    )
    
    transform = PythonOperator(
        task_id = 'transform',
        python_callable=main_transform_new_model_callable,
        dag = dag,
    )
    
    validate = PythonOperator(
        task_id = 'validate',
        python_callable = main_validate_new_model_callable,
        dag=dag
    )
    
    load = PythonOperator(
        task_id = 'load',
        python_callable = main_load_new_model_callable,
        dag=dag
    )
    
  
    

extract >> transform >> validate >> load