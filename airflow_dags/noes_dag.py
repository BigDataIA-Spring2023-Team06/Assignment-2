import pandas as pd
import snowflake.connector
from airflow import DAG
from datetime import datetime, timedelta
import boto3
from snowflake.connector.pandas_tools import write_pandas
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os
from airflow.executors.sequential_executor import SequentialExecutor

# Load the environment variables from the .env file
load_dotenv()

# Access the AWS credentials using the environment variables
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

conn = snowflake.connector.connect(
    user='SANJAYKASHYAP',
    password='Bigdata@23',
    account='iogoldm-vcb38713',
    warehouse='COMPUTE_WH',
    database='SEVIR_META',
    schema='PUBLIC'
)

bucket_name = "noaa-nexrad-level2"
prefix = "ABI-L1b-RadC/"
s3 = boto3.resource("s3")


def check_last_updated_date_from_snowflake():
    # Create a cursor object
    conn.connect()
    cur = conn.cursor()
    query = """SELECT
                "year" AS latest_year,
                MAX("month") AS latest_month,
                MAX("day") AS latest_day
            FROM SEVIR_META.PUBLIC.NOES
            WHERE "year" = (
                SELECT MAX("year") FROM SEVIR_META.PUBLIC.NOES
            )
            GROUP BY "year";"""
    # Execute the SELECT statement to get the last N records from the table
    cur.execute(query)
    conn.close()
    # Fetch the results as a list of tuples
    results = cur.fetchall()

    # Close the cursor and the database connection
    cur.close()
    t = [int(i) for i in results[0]]
    last_updated = datetime(t[0], 1, 1) + timedelta(t[1] - 1) + timedelta(hours=t[2])
    return last_updated


def get_metadata_and_store(s3, bucket_name, last_updated):
    names = []
    #extract year from last_updated
    year = last_updated.year
    paginator = s3.meta.client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name,Prefix=str(year))
    # Loop through each page of objects
    for page in pages:
        for item in page['Contents']:
            names.append(item['Key'])
            # Parse the file name to get the year and day of year
            part = item['Key']
            parts = part.split("/")
            t = (int(parts[0]), int(parts[1]),int(parts[2]))
            file_date = datetime(*t)
            # Check if the file is newer than the last updated date in Snowflake
            if file_date > last_updated:
                names.append(item['Key'])
            else:
                pass
    return names


def metadata_data_frame(names):
    data = pd.DataFrame([i.split('/') for i in names], columns=['Year','Month','Day','Station_Name','File_Name'])
    data.drop('File_Name', axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    return data

def write_to_snowflake(data):
    conn.connect()
    data.columns = map(lambda x: str(x).lower(), data.columns)
    success, nchunks, nrows, _ = write_pandas(conn, data, 'NOES')
    conn.close()

#Define the DAG

default_args = {
    'owner': 'team6',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 1),
    'email': 'mohan.ku@northeastern.edu',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'executer' : SequentialExecutor()
}

dag = DAG('noes_dag',
            default_args=default_args,
            description='Nexrad metadata from S3 to Snowflake',
            schedule_interval="0 0 * * *", # Run once a day at midnight,,
            catchup=False
            )

# Define the tasks
check_last_metadata_update_on_snowflake = PythonOperator(
    task_id='check_last_updated_date_from_snowflake',
    python_callable=check_last_updated_date_from_snowflake,
    dag=dag
)

get_metadata_from_s3 = PythonOperator(
    task_id='get_metadata_and_store',
    python_callable=get_metadata_and_store,
    op_kwargs={'s3': s3, 'bucket_name': bucket_name, 'last_updated': check_last_metadata_update_on_snowflake},
    dag=dag
)

metadata_to_dataframe = PythonOperator(
    task_id='metadata_data_frame',
    python_callable=metadata_data_frame,
    op_kwargs={'names': get_metadata_from_s3},
    dag=dag
)

write_to_snowflake = PythonOperator(
    task_id='write_to_snowflake',
    python_callable=write_to_snowflake,
    op_kwargs={'data': metadata_to_dataframe},
    dag=dag
)

check_last_metadata_update_on_snowflake >> get_metadata_from_s3 >> metadata_to_dataframe >> write_to_snowflake

