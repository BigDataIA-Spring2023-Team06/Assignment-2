import boto3
import sqlite3
import pandas as pd

s3 = boto3.resource("s3")
bucket = s3.Bucket("noaa-goes18")

names = []

for obj in bucket.objects.filter(Prefix="ABI-L1b-RadC/2022"):
    names.append(obj.key)
for obj in bucket.objects.filter(Prefix="ABI-L1b-RadC/2023"):
    names.append(obj.key)


df_names = [i.split('/') for i in names]

data = pd.DataFrame(df_names,columns=['Product','Year','Day','Hour','File Name'])

#Drop irrelavant columns
data.drop('Product', axis=1,inplace=True)
data.drop('File Name', axis=1,inplace=True)

# Connect to SQLite database
db_engine = sqlite3.connect("metadata_db/s3_metadata_goes.db")

#  Write the data frame to the SQLite database
data.to_sql("goes", db_engine, if_exists="replace")

# Close the connections
db_engine.close()
