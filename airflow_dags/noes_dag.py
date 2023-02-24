import boto3
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Define the S3 bucket and prefix
bucket_name = "noaa-goes18"
prefix = "ABI-L1b-RadC/"

# Define the database connection details
connection_id = "sevirteam6rds"
table_name = "goes_meta"

# Set up the AWS S3 resource and hook
s3 = boto3.resource("s3")
s3_hook = S3Hook(aws_conn_id="aws_default")

# Set up the MySQL hook
mysql_hook = MySqlHook(mysql_conn_id=connection_id)

# Define the function to retrieve the metadata and store it in the RDS database
def get_metadata_and_store():
    # Get the list of all files in the S3 bucket for each year
    names = []
    for year in range(2017, 2024):
        prefix_year = f"{prefix}{year}"
        for obj in s3.Bucket(bucket_name).objects.filter(Prefix=prefix_year):
            names.append(obj.key)

    # Extract the year, day of year, and hour metadata from the file paths
    data = pd.DataFrame([i.split('/') for i in names], columns=['Product Name','Year','Day','Hour','File Name'])
    data.drop('Product Name', axis=1, inplace=True)
    data.drop('File Name', axis=1, inplace=True)
    data['datetime'] = pd.to_datetime(data['Year'] + ' ' + data['Day'], format='%Y %j')
    data['year'] = data['datetime'].dt.year
    data['day_of_year'] = data['datetime'].dt.dayofyear
    data.drop('datetime', axis=1, inplace=True)

    # Store the metadata into the RDS database
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (year INT, day_of_year INT, hour INT)")
    cursor.executemany(f"INSERT INTO {table_name} (year, day_of_year, hour) VALUES (%s, %s, %s)", data.values.tolist())
    conn.commit()
    cursor.close()

# Define the DAG
default_args = {
    "owner": "me",
    "start_date": datetime(2023, 2, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "abi_l1b_metadata",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:

    # Define a BashOperator to create the RDS table if it doesn't exist
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"echo 'CREATE TABLE IF NOT EXISTS {table_name} (year INT, day_of_year INT, hour INT)' | mysql --host={mysql_hook.get_uri().split('@')[1]} --port={mysql_hook.get_sqlalchemy_engine().url.port} --user={mysql_hook.get_sqlalchemy_engine().url.username} --password={mysql_hook.get_sqlalchemy_engine().url.password} --database={mysql_hook.get_sqlalchemy_engine().url.database}",
    )

    # Define a PythonOperator to get the metadata and store it in the RDS database
    get_metadata = PythonOperator(
        task_id="get_metadata",
        python_callable=get_metadata_and_store,
    )

    # Set up the dependencies
    create_table >> get_metadata
