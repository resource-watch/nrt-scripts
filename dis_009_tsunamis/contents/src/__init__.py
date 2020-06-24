import logging
import sys
import os
import requests as req
import datetime
import pandas as pd
import cartoframes
import requests
import numpy as np

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_009_tsunamis'

# url for tsunami data
SOURCE_URL = "https://ngdc.noaa.gov/nndc/struts/results?type_0=Exact&query_0=$ID&t=101650&s=69&d=59&dfn=tsevent.txt"

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '2fb159b3-e613-40ec-974c-21b22c930ce4'

'''
FUNCTIONS FOR ALL DATASETS

The functions below must go in every near real-time script.
Their format should not need to be changed.
'''

def lastUpdateDate(dataset, date):
    '''
    Given a Resource Watch dataset's API ID and a datetime,
    this function will update the dataset's 'last update date' on the API with the given datetime
    INPUT   dataset: Resource Watch API dataset ID (string)
            date: date to set as the 'last update date' for the input dataset (datetime)
    '''
    # generate the API url for this dataset
    apiUrl = f'http://api.resourcewatch.org/v1/dataset/{dataset}'
    # create headers to send with the request to update the 'last update date'
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    # create the json data to send in the request
    body = {
        "dataLastUpdated": date.isoformat() # date should be a string in the format 'YYYY-MM-DDTHH:MM:SS'
    }
    # send the request
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''

def create_geom(lat, lon):
    ''' 
    Create geometry using latitude and longitude information from source data
    INPUT   lat: latitude of the tsunami event (string)
            lon: longitude of the tsunami event (string)
    RETURN  geom: geometry of the tsunami event (geojson)
    ''' 
    # check if there is input data for latitude
    if lat:
        # create geometry using latitude and longitude information
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
    Retrive data from source url, create a dataframe, and return the processed dataframe
    RETURN  df: dataframe with data (dataframe object)
    """

    # get data from source url through a request response JSON
    data = req.get(SOURCE_URL).text
    # split data based on new lines to get each rows as a separate value
    data = data.split('\n')
    # split each rows using tab character to separate out the columns
    lines = [line.split('\t') for line in data]
    # get header from first row
    header = lines[0]
    # get all rows of data (all rows that come after the header)
    rows = lines[1:]
    # create a pandas dataframe using the data
    df = pd.DataFrame(rows)
    # specify column names for the dataframe using header
    df.columns = header
    # create the geomtery for each data point using 'LATITUDE' and 'LONGITUDE' columns from source data
    # add the geometry to a new column in pandas dataframe
    df['the_geom'] = list(map(lambda coords: create_geom(*coords), zip(df['LATITUDE'],df['LONGITUDE'])))
    # specify column names for columns containing string data
    text_cols = ['the_geom', 'COUNTRY', 'STATE', 'LOCATION_NAME']
    # find numeric columns (all columns that are not in the text_cols list)
    number_cols = [x for x in df.columns if x not in text_cols]
    # check if there are any empty entries; replace them with NAN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    # loop through each numeric columns
    for col in number_cols:
        # convert those columns in dataframe to numeric type
        # invalid parsing will be set as NaN
        df[col] =  pd.to_numeric(df[col], errors='coerce')

    return(df)

def get_most_recent_date(df):
    '''
    Find the most recent date of data in the Carto table using the parsed dataframe
    INPUT   df: dataframe that was written to the Carto table (dataframe object)
    RETURN  most_recent_date: most recent date of data in the Carto table, 
            found using the 'YEAR', 'MONTH' and 'DAY' column of the dataframe (datetime object)
    '''
    #convert columns associated with date values to numeric for sorting
    table.YEAR = pd.to_numeric(table.YEAR, errors='coerce')
    table.MONTH = pd.to_numeric(table.MONTH, errors='coerce')
    table.DAY = pd.to_numeric(table.DAY, errors='coerce')
    # sort the table by the 'YEAR', 'MONTH' and 'DAY' column
    sorted_table = table.sort_values(by=['YEAR', 'MONTH', 'DAY'], ascending=False).reset_index()
    # get the first value from 'YEAR', 'MONTH' and 'DAY' column
    # those will represent the most recent year, month, day
    year = int(sorted_table['YEAR'][0])
    month = int(sorted_table['MONTH'][0])
    day = int(sorted_table['DAY'][0])
    # create a datetime object using the most recent year, month, day
    most_recent_date = datetime.date(year, month, day)

    return most_recent_date

def updateResourceWatch(df):
    '''
    This function should update Resource Watch to reflect the uploaded data.
    This may include updating the 'last update date' and updating any dates on layers
    '''
    # Update dataset's last update date on Resource Watch
    most_recent_date = get_most_recent_date(df)
    lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # create a CartoContext object that is authenticated against our CARTO account
    # it will be used to write dataframe to Carto table
    cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_USER),
                                  api_key=CARTO_KEY)

    # fetch data from FTP, dedupe, process
    df = processData()

    # get the number of rows in the dataframe
    num_rows = df.shape[0]
    # write the dataframe to the Carto table, overwriting existing data
    cc.write(df, CARTO_TABLE, overwrite=True, privacy='public')

    # Update Resource Watch
    updateResourceWatch(df)

    # Notify results
    logging.info('Existing rows: {}'.format(num_rows))
    logging.info("SUCCESS")
