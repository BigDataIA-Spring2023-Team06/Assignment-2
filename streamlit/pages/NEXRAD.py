import sqlite3
import streamlit as st
import time as tm
from datetime import datetime, timedelta,time, date
import pandas as pd
# import helper_functions.goes_module
import helper
import openpyxl
import requests

conn = sqlite3.connect('metadata_db/s3_metadata_noes.db')
cursor = conn.cursor()
cursor.execute("SELECT Year,Month,Day,Station_Name FROM noes;")
dates_stations = cursor.fetchall()
conn.close()

headers = {"Content-Type": "application/json"}
api_host = "http://34.138.242.155:8000"

state_codes_usa = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

station_codes_usa = {
    'AK': ['BETHEL FAA-PABC', 'SITKA-PACG', 'NOME-PAEC', 'ANCHORAGE-PAHG', 'MIDDLETON ISLAND-PAIH', 'KING SALMON-PAKC', 'FAIRBANKS-PAPD'],
    'AL': ['BIRMINGHAM-KBMX', 'FORT RUCKER-KEOX', 'HUNTSVILLE-KHTX', 'MOBILE-KMOB', 'MAXWELL AFB-KMXX'], 
    'AR': ['LITTLE ROCK-KLZK', 'FORT SMITH-KSRX'], 
    'AZ': ['TUCSON-KEMX', 'FLAGSTAFF-KFSX', 'PHOENIX-KIWA', 'YUMA-KYUX', 'PHOENIX-TPHX'], 
    'CA': ['BEALE AFB-KBBX', 'EUREKA-KBHX', 'SACRAMENTO-KDAX', 'EDWARDS-KEYX', 'SAN JOAQUIN VALLEY-KHNX', 'SAN FRANCISCO-KMUX', 'SAN DIEGO-KNKX', 'SANTA ANA MOUNTAINS-KSOX', 'VANDENBERG AFB-KVBX', 'LOS ANGELES-KVTX'], 
    'CO': ['DENVER FRONT RANGE AP-KFTG', 'GRAND JUNCTION-KGJX', 'PUEBLO-KPUX', 'DENVER-TDEN'], 
    'DE': ['DOVER AFB-KDOX'], 
    'FL': ['MIAMI-KAMX', 'KEY WEST-KBYX', 'EGLIN AFB-KEVX', 'JACKSONVILLE-KJAX', 'MELBOURNE-KMLB', 'TAMPA-KTBW', 'TALLAHASSEE-KTLH', 'FT LAUDERDALE-TFLL', 'ORLANDO INTERNATIONAL-TMCO', 'MIAMI-TMIA', 'WEST PALM BEACH-TPBI', 'TAMPA-TTPA'], 
    'GA': ['ATLANTA-KFFC', 'ROBINS AFB-KJGX', 'MOODY AFB-KVAX', 'ATLANTA-TATL'], 'GU': ['ANDERSEN AFB AGANA-PGUA'], 
    'HI': ['SOUTH KAUAI-PHKI', 'KAMUELA-PHKM', 'MOLOKAI-PHMO', 'SOUTH SHORE-PHWA'], 'IA': ['DES MOINES-KDMX', 'DAVENPORT-KDVN'], 'ID': ['BOISE-KCBX', 'POCATELLO-KSFX'], 'IL': ['LINCOLN-KILX', 'CHICAGO-KLOT', 'CHICAGO MIDWAY-TMDW', 'CHICAGO OHARE-TORD'], 
    'IN': ['INDIANAPOLIS-KIND', 'FORT WAYNE-KIWX', 'EVANSVILLE-KVWX', 'INDIANAPOLIS-TIDS'], 
    'KS': ['DODGE CITY-KDDC', 'GOODLAND-KGLD', 'WICHITA-KICT', 'TOPEKA-KTWX', 'WICHITA-TICH'], 
    'KY': ['FORT CAMPBELL-KHPX', 'JACKSON-KJKL', 'LOUISVILLE-KLVX', 'PADUCAH-KPAH', 'COVINGTON-TCVG', 'LOUISVILLE-TSDF'], 
    'LA': ['LAKE CHARLES-KLCH', 'NEW ORLEANS-KLIX', 'FORT POLK-KPOE', 'SHREVEPORT-KSHV', 'NEW ORLEANS-TMSY'], 
    'MA': ['BOSTON-KBOX', 'BOSTON-TBOS'], 
    'MD': ['ANDREWS AFB-TADW', 'BALTIMORE WASHINGTON-TBWI', 'WASHINGTON NATIONAL-TDCA'], 
    'ME': ['HOULTON-KCBW', 'PORTLAND-KGYX'], 'MI': ['GAYLORD-KAPX', 'DETROIT-KDTX', 'GRAND RAPIDS-KGRR', 'MARQUETTE-KMQT', 'DETROIT-TDTW'], 
    'MN': ['DULUTH-KDLH', 'MINNEAPOLIS-KMPX', 'MINNEAPOLIS-TMSP'], 'MO': ['KANSAS CITY-KEAX', 'ST LOUIS-KLSX', 'SPRINGFIELD-KSGF', 'KANSAS CITY-TMCI', 'ST LOUIS-TSTL'], 
    'MS': ['JACKSON BRANDON-KDGX', 'COLUMBUS AFB-KGWX', 'MEMPHIS-TMEM'], 'MT': ['BILLINGS-KBLX', 'GLASGOW-KGGW', 'MISSOULA-KMSX', 'GREAT FALLS-KTFX'], 
    'NC': ['WILMINGTON-KLTX', 'MOREHEAD CITY-KMHX', 'RALEIGH DURHAM-KRAX', 'CHARLOTTE-TCLT', 'RALEIGH-TRDU'], 
    'ND': ['BISMARCK-KBIS', 'MINOT AFB-KMBX', 'GRAND FORKS-KMVX'], 
    'NE': ['NORTH PLATTE-KLNX', 'OMAHA-KOAX', 'HASTINGS-KUEX'], 
    'NJ': ['PHILADELPHIA-KDIX', 'NEWARK-TEWR', 'PHILADELPHIA-TPHL'], 
    'NM': ['ALBUQUERQUE-KABX', 'EL PASO-KEPZ', 'CANNON AFB-KFDX', 'HOLLOMAN AFB-KHDX'], 
    'NV': ['LAS VEGAS-KESX', 'ELKO-KLRX', 'RENO-KRGX', 'LAS VEGAS-TLAS'], 
    'NY': ['BINGHAMTON-KBGM', 'BUFFALO-KBUF', 'ALBANY-KENX', 'NEW YORK CITY-KOKX', 'FORT DRUM-KTYX', 'NEW YORK CITY JFK-TJFK'], 
    'OH': ['CLEVELAND-KCLE', 'CINCINNATI-KILN', 'COLUMBUS-TCMH', 'DAYTON-TDAY', 'CLEVELAND-TLVE'], 
    'OK': ['ROC FAA REDUNDANT RDA 1-KCRI', 'ALTUS AFB-KFDR', 'TULSA-KINX', 'NORMAN NSSL-KOUN', 'OKLAHOMA CITY-KTLX', 'VANCE AFB-KVNX', 'NORMAN WFO-TOKC', 'TULSA-TTUL'], 
    'OR': ['MEDFORD-KMAX', 'PENDLETON-KPDT', 'PORTLAND-KRTX'], 'PA': ['STATE COLLEGE-KCCX', 'PITTSBURGH-KPBZ', 'PITTSBURGH-TPIT'], 
    'PR': ['RAFAEL HERNANDEZ AIRPORT-TJBQ', 'JOSE APONTE DE LA TORRE AIRPOR-TJRV', 'SAN JUAN-TJUA', 'SAN JUAN-TSJU'], 
    'SC': ['COLUMBIA-KCAE', 'CHARLESTON-KCLX', 'GREER-KGSP'], 
    'SD': ['ABERDEEN-KABR', 'SIOUX FALLS-KFSD', 'RAPID CITY-KUDX'], 
    'TN': ['KNOXVILLE-KMRX', 'MEMPHIS-KNQA', 'NASHVILLE-KOHX', 'NASHVILLE-TBNA'], 'TX': ['AMARILLO-KAMA', 'BROWNSVILLE-KBRO', 'CORPUS CHRISTI-KCRP', 'LAUGHLIN AFB-KDFX', 'DYESS AFB-KDYX', 'AUSTIN SAN ANTONIO-KEWX', 'DALLAS-KFWS', 'FORT HOOD-KGRK', 'HOUSTON-KHGX', 'LUBBOCK-KLBB', 'MIDLAND ODESSA-KMAF', 'SAN ANGELO-KSJT', 'DALLAS LOVE FIELD-TDAL', 'DALLAS FT WORTH-TDFW', 'HOUSTON HOBBY-THOU', 'HOUSTON INTERNATIONAL-TIAH'], 
    'UT': ['CEDAR CITY-KICX', 'SALT LAKE CITY-KMTX', 'SALT LAKE CITY-TSLC'], 
    'VA': ['NORFOLK RICH-KAKQ', 'ROANOKE-KFCX', 'STERLING-KLWX', 'WASHINGTON DULLES-TIAD'], 
    'VT': ['BURLINGTON-KCXX'], 'WA': ['SEATTLE-KATX', 'LANGLEY HILL NW WASHINGTON-KLGX', 'SPOKANE-KOTX'], 
    'WI': ['LA CROSSE-KARX', 'GREEN BAY-KGRB', 'MILWAUKEE-KMKX', 'MILWAUKEE-TMKE'], 
    'WV': ['CHARLESTON-KRLX'], 
    'WY': ['CHEYENNE-KCYS', 'RIVERTON-KRIW']}

selected_state = ''
# "st.session_state object:" , st.session_state
def get_state_from_station(stations):
    
    state_codes = pd.read_excel("pages/nexrad.xlsx").dropna()
    state_codes = state_codes[["NAME", "ST"]]
    #print(set(state_codes[state_codes["NAME"].isin(stations)]["ST"]))
    return set(state_codes[state_codes["NAME"].isin(stations)]["ST"])


   
def time_format(strtime):
    time_list = strtime.split('-')
    year = time_list[0]
    month = time_list[1]
    day = time_list[2]
    
    if len(month) == 1:
        month = "0" + month
    if len(day) == 1:
        day = "0" + day  
    return year + "-" + month + "-" + day


def add_to_session_state(new, value):
    if new not in st.session_state:
        st.session_state[new] = value


def get_stations_from_state(state, station_names):
    stations = station_codes_usa[selected_state]
    stations_in_db = []
    for station in stations:
        for station_name in station_names:
            #print(station_name)
            if station.split('-')[1] in station_name:
                stations_in_db.append(station)
    return stations_in_db

def format_hour(hour):
    if len(hour) == 1:
        hour = "0" + hour
    return hour

    
# Create a list of datetime objects for the dates retrieved from the database
datetime_dates = [datetime(date[0], date[1], date[2]) for date in dates_stations]

states = []
files = []
stations = []
print("##############################################################FIRST RUN##########################")

st.title("NEXRAD")

##########Refresh Button#######
reset = st.button("Reset")
if reset:
    for i in st.session_state.keys():
        del st.session_state[i]

##########To Select Date
selected_date = st.date_input("Select a date:", min_value=min(datetime_dates), max_value=date.today(), key = 'nexrad_date_ip')    
add_to_session_state("select_date", str(st.session_state["nexrad_date_ip"]))
submit_date = st.button("Select Date")

if submit_date:    
    st.session_state["select_date"] = str(st.session_state["nexrad_date_ip"]) 

        ####Loop to filter dates according to station
    filtered_dates_stations = []
    for date_station in dates_stations:
        if (time_format(str(date_station[0]) + "-" + str(date_station[1]) + "-" + str(date_station[2])) ==  st.session_state['select_date']):
            filtered_dates_stations.append(date_station)
        
    station_names = [date_station[3] for date_station in filtered_dates_stations]
    add_to_session_state("filtered_dates_stations", filtered_dates_stations)
    add_to_session_state("station_names_by_date", station_names)
    states = sorted(get_state_from_station(station_names))
    add_to_session_state("states", states)


try:
    states = list(st.session_state["states"])
    selected_state = st.selectbox("Select a state:", states, key='nexrad_selected_state')
    add_to_session_state("selected_state", selected_state)
except:
    print("In Except First RUN")
    
submit_state = st.button("Select State")


if submit_state:
    st.session_state["selected_state"] = selected_state
    
    if "station_names_by_date" in st.session_state:
        station_names_date = list(st.session_state["station_names_by_date"])
        stations_in_db = get_stations_from_state(st.session_state["selected_state"], station_names_date) 
        add_to_session_state("stations", stations_in_db)
        
    
if "stations" in st.session_state:    
    stations = st.session_state["stations"]
    
selected_station = st.selectbox('Stations', stations, key = 'nexrad_select_station')  
add_to_session_state("selected_station", selected_station)  
hour = format_hour(str(st.selectbox('Select Hour', [*range(0, 24)], key = 'nexrad_select_hour')))
print(hour)
submit_station = st.button("Submit Station")

#add_to_session_state("selected_hour", selected_hour)

if submit_station:
    
    try:
        st.session_state["selected_station"] = selected_station
        year = st.session_state["select_date"].split('-')[0]
        month = st.session_state["select_date"].split('-')[1]
        day = st.session_state["select_date"].split('-')[2]
        station = st.session_state["selected_station"].split("-")[-1]
# response_goes_files = requests.get(f"{api_host}/get_files_goes/{year}/{month}/{day}/{hour}")
#             selected_files = dict(response_goes_files.json())["list_of_files"]
# "/get_files_noaa/{station}/{year}/{month}/{day}/{hour}"
        response_nexrad_files = requests.get(f"{api_host}/get_files_noaa/{station}/{year}/{month}/{day}/{hour}")
        print(response_nexrad_files.json())
        selected_files = dict(response_nexrad_files.json())['list of files']
        add_to_session_state("file_list", selected_files)

    except:
        st.write("Please Enter All the Details")



if "file_list" in st.session_state:    
    files = st.session_state["file_list"]

print(files)
selected_file = st.selectbox('Files Available', files, key = "nexrad_file_selected")
add_to_session_state("selected_file", selected_file)

submit_file = st.button("Generate URL")
if submit_file:
    try:
        payload = {"src_file_key":selected_file, "src_bucket_name":"noaa-nexrad-level2", "dst_bucket_name":"goes-team6", "dataset":"NEXRAD"} 
        response_s3 = requests.post(f"{api_host}/copy_to_s3/", params=payload)
        response_s3 = response_s3.json()
        st.write("Download from MI-6 Bucket " + response_s3[0])
        st.write("Download from GOES Bucket " + response_s3[1])
        st.session_state["selected_station"] = selected_station
        # cw_logs.add_logs_nexrad_search(st.session_state["selected_station"], 00, st.session_state["selected_state"], st.session_state["selected_station"], st.session_state["selected_file"], url_noes[1], url_noes[0])
    except:
        st.write("Please Enter All the Details")    
    
    for i in st.session_state.keys():
        del st.session_state[i]


with st.form("url_generator"):
    st.title("Search by Filename")
    filename = st.text_input("Please enter filename")
    url_button = st.form_submit_button("Generate URL")
    if url_button:
        x = filename[4:8] + "/" + filename[8:10] + "/" + filename[10:12] + "/" + filename[0:4] + "/" + filename
        flag = 0
        
        if(not helper.validate_filename_nexrad(filename)):
            st.write("File Name Format Not Correct")
            #cw_logs.add_logs_file("NEXRAD", filename, "File Name Format Not Correct")
        elif(not helper.file_exists("noaa-nexrad-level2", x)):
            st.write("File Does Not Exist")
            #cw_logs.add_logs_file("NEXRAD", filename, "File Does Not Exist")		
        else:
            filename_url = requests.get(f"{api_host}/get_url_nexrad_original/{filename}")
            url = dict(filename_url.json())["original url"]
            st.write(url)