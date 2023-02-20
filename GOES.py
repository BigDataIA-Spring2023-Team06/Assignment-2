import sqlite3
import streamlit as st
import time as tm
from datetime import datetime, timedelta,time, date
# import helper_functions.goes_module
import requests
import helper

# Connect to the database
conn = sqlite3.connect('metadata_db/s3_metadata_goes.db')
# Create a cursor object
cursor = conn.cursor()

# Execute a query to retrieve the year, day of the year, and hour of the day from the database
cursor.execute("SELECT Year, Day, Hour FROM goes;")


# Fetch all the results of the query
data = cursor.fetchall()

# Close the connection
conn.close()

# Convert the year, day, and hour into a datetime object and store in a dict
dates_dict = {datetime(int(row[0]), 1, 1) + timedelta(int(row[1]) - 1, hours=int(row[2])): row for row in data}

# Create a set of unique dates
unique_dates = set(d.date() for d in dates_dict.keys())


file_to_download = ''
dest_url = ''
headers = {"Content-Type": "application/json"}
api_host = "http://34.138.242.155:8000"

with st.form(key="my_form"):
    st.title("GOES-Docker")
    # Create a date input widget
    selected_date = st.date_input("Date:", min_value=min(unique_dates), max_value=date.today())
    # selected_time = st.time_input("Time:")
    if selected_date:
        submit_button = st.form_submit_button("Next")
    hours = [datetime.time(d) for d in dates_dict.keys() if d.date() == selected_date]
    hours_stripped = sorted(set(hours))

    # Check if the selected date is in the database
    if not hours_stripped:
        st.warning("No data found for the selected date.")
    else:
        # Create a time input widget
        hours_stripped.insert(0,'Select')
        selected_time = 'Select'
        selected_time = st.selectbox("Hour", hours_stripped)

        search_fields = st.form_submit_button(label="Search")
        # Combine the selected date and time into a single datetime object
        if type(selected_time) == str:
            pass
        else:
            selected_datetime = datetime.combine(selected_date, selected_time)
            year = selected_date.strftime("%Y")
            month = selected_date.strftime("%m")
            day = selected_date.strftime("%d")
            hour = selected_time.strftime("%H")

            #APIed code 
            response_goes_files = requests.get(f"{api_host}/get_files_goes/{year}/{month}/{day}/{hour}")
            selected_files = dict(response_goes_files.json())["list_of_files"]
            st.write('Number of files found:',len(selected_files))
            file_to_download = st.selectbox("Please select file for download: ",selected_files)
            download = st.form_submit_button("Get URL")
            url = ("1", "1")
            if download:

                payload = {"src_file_key":file_to_download, "src_bucket_name":"noaa-goes18", "dst_bucket_name":"goes-team6", "dataset":"GOES"}
                print(payload)
                response_s3 = requests.post(f"{api_host}/copy_to_s3/", params=payload)
                response_s3 = response_s3.json()
                st.write("Destination URL: " + response_s3[1])
                st.write("Source URL: " + response_s3[0])

            # Check if the selected datetime is in the database
            if selected_datetime not in dates_dict:
                st.warning("No data found for the selected date and time.")
            else:
                # Show the data for the selected datetime
                st.write("Data for selected date and hour is present on the metadata db",selected_date, selected_datetime.hour)
                #cw_logs.add_logs_goes_search(selected_date, hour, file_to_download, url[1], url[0])

with st.form("url_generator"):
    st.title("Search by Filename")
    filename = st.text_input("Please enter filename")
    url_button = st.form_submit_button("Generate URL")
    if url_button:
        try:
            split = filename.split('_')
            timeStamp = split[4][1:]
            year = timeStamp[:4]
            day = timeStamp[4:7] 
            hour = timeStamp[7:9]

            #APIed code
            x = "ABI-L1b-RadC" + "/" + timeStamp[:4] + "/" + timeStamp[4:7] + "/" +  timeStamp[7:9] + "/" + filename
            if (not helper.validate_filename_goes(filename)):
                st.write("File Format Incorrect")
            elif (not helper.file_exists("noaa-goes18", x)):
                st.write("File Does Not Exist")
#                 cw_logs.add_logs_file("GOES", filename, "File Does Not Exist")
            else:
                filename_url = requests.get(f"{api_host}/get_url_goes_original/{filename}")
                url = dict(filename_url.json())["original url"]
                st.write(url)
#                 cw_logs.add_logs_file("GOES", filename, url)
        except:
            st.write("Invalid File Name")





# http://34.138.242.155:8000/get_url_goes_original/OR_ABI-L1b-RadC-M6C01_G18_s20230100506171_e20230100508546_c20230100508584.nc

#             x = "ABI-L1b-RadC" + "/" + timeStamp[:4] + "/" + timeStamp[4:7] + "/" +  timeStamp[7:9] + "/" + filename
#             # Extracting the timestamp
#             if (not helper.validate_filename_goes(filename)):
#                 st.write("File Format Incorrect")
#             elif (not helper.file_exists("noaa-goes18", x)):
#                 st.write("File Does Not Exist")
#                 cw_logs.add_logs_file("GOES", filename, "File Does Not Exist")
#             else:
#                 url = goes_module.get_url_goes_original(filename)
#                 st.write(url)
#                 cw_logs.add_logs_file("GOES", filename, url)
#         except:
#             st.write("Invalid File Name")


