import boto3
import botocore
import re
 

s3 = boto3.client(
    's3'
)

def file_exists(bucket_name, object_key):
    
    #s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
    except botocore.exceptions.ClientError:
        return False
    return True



def validate_filename_goes(filename):
    pattern = re.compile(r"^OR_ABI-L1b-RadC-M6C\d{2}_G\d{2}_s\d{14}_e\d{14}_c\d{14}\.nc$")
    return bool(pattern.match(filename))