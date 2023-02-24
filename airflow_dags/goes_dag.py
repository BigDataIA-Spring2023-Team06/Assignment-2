import pandas as pd
import snowflake.connector
from airflow import DAG
from datetime import datetime, timedelta
import boto3
from snowflake.connector.pandas_tools import write_pandas
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import os
from airflow.executors.local_executor import LocalExecutor
import great_expectations as ge
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

bucket_name = "noaa-goes18"
prefix = "ABI-L1b-RadC/"
s3 = boto3.resource("s3")

def check_last_updated_date_from_snowflake():
    # Create a cursor object
    cur = conn.cursor()
    query = """WITH maxyear AS (
                    SELECT 
                        CAST(year AS int) AS year_int, 
                        CAST(day AS INT) AS day_int, 
                        CAST(hour AS INT) AS hour_int
                    FROM goes
                    WHERE year = (select max(year) from goes)
                    )

                    SELECT max(year_int),max(day_int),min(hour_int)
                    FROM maxyear;"""
    # Execute the SELECT statement to get the last N records from the table
    cur.execute(query)
    conn.close()
    # Fetch the results as a list of tuples
    results = cur.fetchall()

    # Close the cursor and the database connection
    cur.close()
    t = results[0]
    last_updated = datetime(t[0], 1, 1) + timedelta(t[1] - 1) + timedelta(hours=t[2])
    return last_updated

def get_metadata_and_store(s3, bucket_name, prefix, last_updated):
    names = []
    paginator = s3.meta.client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # Loop through each page of objects
    for page in page_iterator:
        # Loop through each object in the page
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key.endswith(".nc"):
                # Parse the file name to get the year and day of year
                parts = key.split("/")
                t = (int(parts[1]), int(parts[2]),int(parts[3]))
                file_date = datetime(t[0], 1, 1) + timedelta(t[1] - 1) + timedelta(hours=t[2])
                # Check if the file is newer than the last updated date in Snowflake
                if file_date > last_updated:
                    names.append(key)
    return names

def metadata_data_frame(names):
    data = pd.DataFrame([i.split('/') for i in names], columns=['Product Name','Year','Day','Hour','File Name'])
    data.drop('Product Name', axis=1, inplace=True)
    data.drop('File Name', axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    return data

def run_ge_check(data):
    context = ge.data_context.DataContext()
    suite = context.get_expectation_suite('my_suite')
    batch_kwargs = {'path': '/path/to/my/data'}
    batch = context.get_batch(batch_kwargs, suite)
    results = context.run_validation_operator('action_list_operator', assets_to_validate=[batch])
    return results

def write_to_snowflake(data):
    conn.connect()
    data.columns = map(lambda x: str(x).upper(), data.columns)
    success, nchunks, nrows, _ = write_pandas(conn, data, 'GOES')
    conn.close()


#Define the DAG

default_args = {
    'owner': 'team6',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 1),
    'email': 'mohan.ku@northeastern.edu',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '0 0 * * *',
    'executor': LocalExecutor()
}


dag = DAG('goes_dag',
            default_args=default_args,
            description='GOES DAG',
            catchup=False)

# Define the tasks

# Task 1: Check the last updated date from Snowflake
check_last_updated_date_from_snowflake = PythonOperator(
    task_id='check_last_updated_date_from_snowflake',
    python_callable=check_last_updated_date_from_snowflake,
    dag=dag
)

# Task 2: Get the metadata and store it in a dataframe

get_metadata_and_store = PythonOperator(
    task_id='get_metadata_and_store',
    python_callable=get_metadata_and_store,
    op_kwargs={'s3': s3, 'bucket_name': bucket_name, 'prefix': prefix, 'last_updated': check_last_updated_date_from_snowflake},
    dag=dag
)

# Task 3: Create a data frame from the metadata

metadata_data_frame = PythonOperator(
    task_id='metadata_data_frame',
    python_callable=metadata_data_frame,
    op_kwargs={'names': get_metadata_and_store},
    dag=dag
)

# Task 4: Run the Great Expectations check

run_ge_check = PythonOperator(
    task_id='run_ge_check',
    python_callable=run_ge_check,
    op_kwargs={'data': metadata_data_frame},
    dag=dag
)

# Task 5: Write the data to Snowflake

write_to_snowflake = PythonOperator(
    task_id='write_to_snowflake',
    python_callable=write_to_snowflake,
    op_kwargs={'data': metadata_data_frame},
    dag=dag
)

# Set the dependencies

# check_last_updated_date_from_snowflake >> get_metadata_and_store >> metadata_data_frame >> run_ge_check >> write_to_snowflake

check_last_updated_date_from_snowflake.set_downstream(get_metadata_and_store)
get_metadata_and_store.set_downstream(metadata_data_frame)
metadata_data_frame.set_downstream(write_to_snowflake)
metadata_data_frame.set_upstream(run_ge_check)

