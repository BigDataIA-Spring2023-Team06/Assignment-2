from datetime import datetime, timedelta
from fastapi import Depends, FastAPI, HTTPException, status
from helper_functions import goes_module as gm
from helper_functions import helper
from helper_functions import noes_module as nm
# from jose import JWTError, jwt
# from passlib.context import CryptContext
# from pydantic import BaseModel

app = FastAPI()



#API to get the list of files GOES  
@app.get("/get_files_goes/{year}/{month}/{day}/{hour}")
async def get_files_goes_api(year, month, day, hour):
    return {"list_of_files":gm.get_files_goes(year, month, day, hour)}

# #POST API to copy files to s3   
@app.post("/copy_to_s3/") 
async def copy_to_s3_goes(src_file_key, src_bucket_name, dst_bucket_name, dataset):
    urls = helper.copy_to_s3(src_file_key, src_bucket_name, dst_bucket_name, dataset)
    return {"url": urls}


@app.get("/map_visualization/{station}")
async def plot_map_viz(station):
    name, lat, lon = helper.map_viz(station)
    return {"name": name, "lat": lat, "lon": lon}

@app.get("/get_files_noaa/{station}/{year}/{month}/{day}/{hour}")
async def get_files_noaa_api(station, year, month, day, hour):
    return {"list of files": nm.get_files_noaa(station, year, month, day, hour)}
    
@app.get("/get_url_nexrad_original/{filename}")
async def get_url_nexrad_original(filename):
    return {"original url": nm.get_url_nexrad_original(filename)}

@app.get("/get_url_goes_original/{filename}")
async def get_url_goes_original(filename):
    return {"original url": gm.get_url_goes_original(filename)}










    




 






    



