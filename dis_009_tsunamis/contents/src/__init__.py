import logging
import sys
import os
import requests as req
import datetime
import pandas as pd
import cartoframes
import requests
import numpy as np
import math


### Constants
SOURCE_URL = "https://ngdc.noaa.gov/nndc/struts/results?type_0=Exact&query_0=$ID&t=101650&s=69&d=59&dfn=tsevent.txt"

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
LOG_LEVEL = logging.INFO

### Table name and structure
CARTO_TABLE = 'dis_009_tsunamis'
DATASET_ID = '2fb159b3-e613-40ec-974c-21b22c930ce4'
def lastUpdateDate(dataset, date):
   apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
   headers = {
   'Content-Type': 'application/json',
   'Authorization': os.getenv('apiToken')
   }
   body = {
       "dataLastUpdated": date.isoformat()
   }
   try:
       r = requests.patch(url = apiUrl, json = body, headers = headers)
       logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
       return 0
   except Exception as e:
       logging.error('[lastUpdated]: '+str(e))

DATASET_ID =  '2fb159b3-e613-40ec-974c-21b22c930ce4'

def lastUpdateDate(dataset, date):
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    body = {
        "dataLastUpdated": date.isoformat()
    }
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

###
## Accessing remote data
###

def create_geom(lat, lon):
    if lat:
        geom = {
            "type": "Point",
            "coordinates": [
                lon,
                lat
            ]
        }
        return geom
    else:
        return None

def processData():
    """
    Inputs: SOURCE_URL where data is stored
    Actions: Retrives data, creates date column, and returns dataframe
    Output: Dataframe with data
    """

    data = req.get(SOURCE_URL).text
    data = data.split('\n')
    lines = [line.split('\t') for line in data]
    header = lines[0]
    rows = lines[1:]
    df = pd.DataFrame(rows)
    df.columns = header
    df['the_geom'] = list(map(lambda coords: create_geom(*coords), zip(df['LATITUDE'],df['LONGITUDE'])))

    text_cols = ['the_geom', 'COUNTRY', 'STATE', 'LOCATION_NAME']
    number_cols = [x for x in df.columns if x not in text_cols]
    df = df.replace(r'^\s*$', np.nan, regex=True)
    for col in number_cols:
        print(col)
        df[col] =  pd.to_numeric(df[col], errors='coerce')
    return(df)

def get_most_recent_date(table):
    #year = table.sort_values(by=['YEAR', 'MONTH', 'DAY'], ascending=True)['YEAR']
    #convert date values to numeric for sorting
    table.YEAR = pd.to_numeric(table.YEAR, errors='coerce')
    table.MONTH = pd.to_numeric(table.MONTH, errors='coerce')
    table.DAY = pd.to_numeric(table.DAY, errors='coerce')
    #sort by date
    sorted_table = table.sort_values(by=['YEAR', 'MONTH', 'DAY'], ascending=False).reset_index()
    year = int(sorted_table['YEAR'][0])
    month = int(sorted_table['MONTH'][0])
    day = int(sorted_table['DAY'][0])
    most_recent_date = datetime.date(year, month, day)
    return most_recent_date

###
## Application code
###

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)

    ### 1. Authenticate to Carto
    CARTO_USER = os.environ.get('CARTO_USER')
    CARTO_KEY = os.environ.get('CARTO_KEY')


    cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_USER),
                                  api_key=CARTO_KEY)
    ### 2. Fetch data from FTP, dedupe, process
    df = processData()

    num_rows = df.shape[0]
    cc.write(df, CARTO_TABLE, overwrite=True, privacy='public')

    # Get most recent update date
    most_recent_date =  get_most_recent_date(df)
    lastUpdateDate(DATASET_ID, most_recent_date)

    ### 3. Notify results
    logging.info('Existing rows: {}'.format(num_rows))
    logging.info("SUCCESS")
