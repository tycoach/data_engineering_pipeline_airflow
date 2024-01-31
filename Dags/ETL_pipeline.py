import requests
from io import StringIO
from airflow.providers.google.cloud.operators import bigquery
from airflow import AirflowException
from airflow.decorators import dag, task
import requests
import boto3
import pandas as pd
import json
import os
#import configparser
import pandas_gbq
from dotenv import load_dotenv

from google.oauth2 import service_account
from google.cloud import bigquery

##Load Environment Variable from .env file
load_dotenv()
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
bq_project = os.getenv("PROJECT_ID")
bq_dataset = os.getenv("DATASET_ID")

s3_file_path = "s3://data-infra-dump/data/processed/parkproperty.csv"

s3_obj = boto3.resource('s3')

##Extraction Phase
def extract_data(ti) -> None:
    url = "https://data.sfgov.org/resource/gtr9-ntp6.json"
    """
    Download a file from a given URL.

    Parameters:
    - url (str): The URL of the JSON file.

    Returns:
    - Loaded JSON data as a Python dictionary, or None if unsuccessful.
    """
  
    # Make a GET request to the URL
    response = requests.get(url) 
    ti.xcom_push(key = 'extract_data', value = response.json())


##Transformation phase
def transform_data (ti) -> None:
    response = ti.xcom_pull(key='extract_data', task_ids='get_extracted_data')
    df = pd.DataFrame(response)
    df = df.drop(['shape'], axis= 1)
    ti.xcom_push(key = 'transform_json_data',value = df.to_json(orient=('split')))
    
#AWS S3 BUCKET - Datalake
def load_into_aws_s3(ti) -> None:
    response = ti.xcom_pull(key='transform_json_data', task_ids='transform_json_data')

    # Check if response is not None and contains valid data
    if response is not None:
        df = pd.read_json(response, orient='split')

        # Create AWS session with access key and secret key
        session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        # Create S3 resource from the session
        s3_res = session.resource('s3')

        # Convert DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Specify S3 bucket and object name
        s3_bucket_name = "data-infra-dump"
        s3_object_name = 'data/processed/parkproperty.csv'

        # Upload CSV data to S3 with content type 'text/csv'
        s3_res.Object(s3_bucket_name, s3_object_name).put(Body=csv_buffer.getvalue(), ContentType='text/csv')
    else:
        print("No valid data to upload to S3.")


def load_data_into_bigquery(ti) -> None:
    response = ti.xcom_pull(key='transform_json_data', task_ids='transform_json_data')

    # Check if response is not None and contains valid data
    if response is not None:
        # Convert JSON data to DataFrame
        df = pd.read_json(response, orient='split')
       # Save DataFrame to a local CSV file
        local_csv_path = '\Documents\Engineering-Pipeline\Data\parkproperty_local.csv'  # Adjust the file path as needed
        df.to_csv(local_csv_path, index=False)
        # Set the environment variable for service account key file
        credentials = service_account.Credentials.from_service_account_file(
        filename  = "config/data_eng_interns.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
         )
        bigquery.Client(credentials=credentials, project= bq_project)
        # Specify BigQuery dataset and table names
        bigquery_dataset_id = bq_dataset
        bigquery_table_id = 'park_property'
        
        pandas_gbq.to_gbq(
            df,
            f'{bigquery_dataset_id}.{bigquery_table_id}',
            project_id= bq_project,
            table_schema=None,  # You can provide a schema if needed
            credentials= credentials
        )

        print(f'Data loaded into BigQuery table: {bigquery_dataset_id}.{bigquery_table_id}')

        # Delete the local CSV file after loading into BigQuery
        os.remove(local_csv_path)
        print(f'Local CSV file deleted: {local_csv_path}')

    else:
        print("No valid data to load into BigQuery.")

        
