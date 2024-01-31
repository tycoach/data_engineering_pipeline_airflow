from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow import AirflowException
from airflow.decorators import dag, task
from ETL_pipeline import (extract_data, load_into_aws_s3,transform_data,load_data_into_bigquery)


def on_failure_callback(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    log_url = task_instance.log_url
    msg = f"Task {dag_id}.{task_id} failed. Log URL: {log_url}"
    print(msg)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 27),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': on_failure_callback,
}


with DAG (
    default_args=default_args,
    description="Get JSON Data using API endpoint",
    dag_id="get_datafs_api_data",
    start_date=datetime(2024, 1, 31),
    schedule='@daily',
    catchup=False, 
    ) as dag:

   task1 = PythonOperator( task_id = "get_extracted_data" ,
                            python_callable = extract_data,
                            dag = dag
                           
                            )
   task2 = PythonOperator(   task_id = "load_data",
                           python_callable= load_into_aws_s3,
                           dag = dag
                       )
   task3 = PythonOperator ( task_id = "transform_json_data",
                             python_callable= transform_data,
                             dag = dag

                        )
 
   task4 = PythonOperator(task_id="load_data_to_bigquery",
                            python_callable=load_data_into_bigquery,
                            dag=dag
)
   
task1>>task2>>task3>>task4
##Setting Up task dependencies    

if __name__ == "__main__":
    dag.cli()