import requests


input_params = {
    "src_file_key": "ABI-L1b-RadC/2022/213/19/OR_ABI-L1b-RadC-M6C01_G18_s20222131901172_e20222131903544_c20222131903577.nc", 	
    "src_bucket_name" : "noaa-goes18", 
    "dst_bucket_name": "goes-team6",
    "dataset": "GOES",
}

response = requests.post("http://127.0.0.1:8000/copy_to_s3/", params=input_params)

print(response.json())

response_files = requests.get("http://127.0.0.1:8000/get_files_goes/2022/09/11/07")
print(response_files.json())
#print(response_files)