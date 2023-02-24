import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import boto3

conn = snowflake.connector.connect(
    user='SANJAYKASHYAP',
    password='Bigdata@23',
    account='iogoldm-vcb38713',
    warehouse='COMPUTE_WH',
    database='SEVIR_META',
    schema='PUBLIC'
)


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

    # Fetch the results as a list of tuples
    results = cur.fetchall()

    # Close the cursor and the database connection
    cur.close()
    conn.close()
    t = results[0]
    last_updated = datetime(t[0], 1, 1) + timedelta(t[1] - 1) + timedelta(hours=t[2])
    return last_updated


def connect_to_goes_s3():
    # Define the S3 bucket and prefix
    bucket_name = "noaa-goes18"
    prefix = "ABI-L1b-RadC/"
    s3 = boto3.resource("s3")
    return s3, bucket_name, prefix


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

#Define the DAG
dag = DAG(
    dag_id='goes18',
    default_args=default_args,
    description='DAG to ingest GOES-18 data',
    schedule_interval='0 0 * * *',
    start_date=datetime(2020, 12, 1),
    catchup=False
)

# Define the task to check the last updated date in Snowflake
check_last_updated_date = PythonOperator(
    task_id='check_last_updated_date',
    python_callable=check_last_updated_date_from_snowflake,
    dag=dag
)

# Define the task to connect to the S3 bucket
connect_to_goes_s3 = PythonOperator(
    task_id='connect_to_goes_s3',
    python_callable=connect_to_goes_s3,
    dag=dag
)

# Define the task to get the metadata and store it in a list
get_metadata_and_store = PythonOperator(
    task_id='get_metadata_and_store',
    python_callable=get_metadata_and_store,
    op_kwargs={'s3': s3, 'bucket_name': bucket_name, 'prefix': prefix, 'last_updated': last_updated},
    dag=dag
)

# Define the task to create a data frame from the list
metadata_data_frame = PythonOperator(
    task_id='metadata_data_frame',
    python_callable=metadata_data_frame,
    op_kwargs={'names': names},
    dag=dag
)

# Define the task to write the data frame to Snowflake
write_to_snowflake = PythonOperator(
    task_id='write_to_snowflake',
    python_callable=write_to_snowflake,
    op_kwargs={'data': data},
    dag=dag
)

#Set the dependencies between the tasks and upstrean and downstream tasks
check_last_updated_date >> connect_to_goes_s3 >> get_metadata_and_store >> metadata_data_frame >> write_to_snowflake

# Run the DAG
dag.cli()



