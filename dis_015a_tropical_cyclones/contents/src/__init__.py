from __future__ import unicode_literals

import os
import logging
import sys
import urllib
import datetime
import cartoframes
from cartoframes import read_carto
import cartosql
from zipfile import ZipFile
import requests
import geopandas as gpd
import glob


# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_015a_hurricane_tracks'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'iso_time'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# url for cyclone track data
# url_a = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/'
# url_b = 'v04r00/access/shapefile/IBTrACS.since1980.list.v04r00.lines.zip'
url_a = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/'
url_b = 'v04r00/access/shapefile/IBTrACS.last3years.list.v04r00.lines.zip'
SOURCE_URL = url_a + url_b

# maximum number of attempts that will be made to download the data
MAX_TRIES = 5

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
# DATASET_ID = 'b82eab85-0fee-4212-8a7e-ca0b28a16a2f'

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

def gen_uid(row):
    '''
    Generate unique id using Storm Id (SID) and Time (ISO_TIME)
    INPUT   row: rows of the geodataframe for tropical cyclone track data (Series object)
    RETURN  output: formatted rows with additional column for unique id (Series object)
    '''
    # get values from ISO_TIME and format it to have underscores inbetween each time variables 
    tm = row['ISO_TIME'].replace("-", "_").replace(" ", "_").replace(":", "_")
    # get storm id from the column 'SID' and join with the formatted time
    out = row['SID'] + '_' + tm
    return out

def fetch_data():
    '''
    Download shapefile from source url and put that into a geopandas dataframe
    RETURN  gdf: geodataframe for tropical cyclone track data (dataframe)
    '''
    # create a filename for the shapefile that will be downloaded
    tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,'dis_015a_hurricane_tracks-shp'))
    logging.info('Fetching shapefile')

#    try:
#     logging.info('pull data from url and save to tmpfile')
    # pull data from url and save to tmpfile
    urllib.request.urlretrieve(SOURCE_URL, tmpfile)
#     logging.info('unzip source data')
    # unzip source data
    tmpfile_unzipped = tmpfile.split('.')[0]
    zip_ref = ZipFile(tmpfile, 'r')
    zip_ref.extractall(tmpfile_unzipped)
    zip_ref.close()
#     logging.info('load in the polygon shapefile')
    # load in the polygon shapefile
    shapefile = glob.glob(os.path.join(tmpfile_unzipped, '*.shp'))[0]
    logging.info('gpd.read_file(shapefile)')
    gdf = gpd.read_file(shapefile)
    logging.info('generate unique id')
    # generate unique id and the values to a new column in the geodataframe
    gdf['uid'] = gdf.apply(gen_uid, axis=1)
    
    return gdf

#    except Exception as e:
#        logging.info(e)
#        logging.info("Error fetching data")

def processData():
    '''
    Function to download data and upload it to Carto.
    We will first try to get the data for MAX_TRIES then quit
    RETURN  existing_ids: list of existing IDs in the table (list of strings)
            new_rows: number of rows of new data sent to Carto table (integer)
    '''
    # set success to False initially
    success = False
    # initialize tries count as 0
    tries = 0
    # try to get the data from the url for MAX_TRIES 
    while tries < MAX_TRIES and success == False:
        logging.info('Try retrieving cyclone track data, try number = {}'.format(tries))
        try:
            # pull cyclone track data from source url and format the data into a geopandas dataframe
            # download the shapefile from source 
            gdf = fetch_data()
            # set success as True after retrieving the data to break out of this loop
            success = True
        except Exception as inst:
            logging.info(inst)
            logging.info("Error fetching data, trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
    # if we suceessfully collected data from the url
    if success == True:
        # generate credentials to access the table
        creds = cartoframes.auth.Credentials(username=CARTO_USER, api_key=CARTO_KEY, 
        base_url="https://{user}.carto.com/".format(user=CARTO_USER))
        # Check if table exists, create it if it does not
        logging.info('Load the existing table as a geodataframe.')
        # Load the existing table as a geodataframe
        gdf_exist = read_carto('dis_015a_hurricane_tracks_test3', credentials=creds)
        # create a list from the unique id column in geodataframe
        existing_ids = list(gdf_exist['uid'])
        # create a new geodataframe with unique ids that are not already in our Carto table
        gdf_new = gdf[~gdf['uid'].isin(existing_ids)]
        # count the number of new rows to add to the Carto table
        new_rows = gdf_new.shape[0]
        # if we have new data to upload
        if new_rows != 0:
            logging.info('Sending new data to the Carto table')
            # send new rows to the Carto table
            cartoframes.to_carto(gdf_new, CARTO_TABLE, credentials=creds, if_exists='append')
            logging.info('Successfully sent new data to the Carto table')
        else:
            logging.info('Table already upto date')
            
        return(existing_ids, new_rows)

def deleteExcessRows(table, max_rows, time_field):
    ''' 
    Delete rows to bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0
    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], CARTO_USER, CARTO_KEY)
        # get the number of rows that have been dropped from the table
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

    return(num_dropped)

def get_most_recent_date(table):
    '''
    Find the most recent date of data in the specified Carto table
    INPUT   table: name of table in Carto we want to find the most recent date for (string)
    RETURN  most_recent_date: most recent date of data in the Carto table, found in the TIME_FIELD column of the table (datetime object)
    '''
    # get dates in TIME_FIELD column
    r = cartosql.getFields(TIME_FIELD, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
    # turn the response into a list of dates
    dates = r.text.split('\r\n')[1:-1]
    # sort the dates from oldest to newest
    dates.sort()
    # turn the last (newest) date into a datetime object
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')

    return most_recent_date

# def updateResourceWatch():
#     '''
#     This function should update Resource Watch to reflect the new data.
#     This may include updating the 'last update date' and updating any dates on layers
#     '''
#     # Update dataset's last update date on Resource Watch
#     most_recent_date = get_most_recent_date(CARTO_TABLE)
#     lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Fetch, process, and upload new data
    existing_ids, num_new = processData()
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    # Update Resource Watch
    # updateResourceWatch()

    logging.info('SUCCESS')

