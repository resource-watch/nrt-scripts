import logging
import sys
import os
import time
from collections import OrderedDict
import cartosql
import urllib
import pandas as pd
from carto.datasets import DatasetManager
from carto.auth import APIKeyAuthClient
import datetime
import requests
from bs4 import BeautifulSoup

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = True

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'soc_049_water_conflict_map'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'end_dt'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
        ('the_geom', 'geometry'),
        ('uid', 'numeric'),
        ('date', 'text'),
        ('headline', 'text'),
        ('conflict_type', 'text'),
        ('region', 'text'),
        ('description', 'text'),
        ('sources', 'text'),
        ('latitude', 'numeric'),
        ('longitude', 'numeric'),
        ('start_year', 'numeric'),
        ('end_year', 'numeric'),
        ('start_dt', 'text'),
        ('end_dt', 'text')
    ])

# url for water conflict map data
SOURCE_URL = 'http://www.worldwater.org/conflict/php/table-data-scraping.php?jstr={{%22region%22:%22%%22,%22conftype%22:%22%%22,%22epoch%22:%22-5000,{current_year}%22,%22search%22:%22%22}}'.format(current_year = datetime.date.today().year)

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '24928aa3-28d3-457c-ad2a-62f3c83ef663'

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
FUNCTIONS FOR CARTO DATASETS

The functions below must go in every near real-time script for a Carto dataset.
Their format should not need to be changed.
'''

def checkCreateTable(table, schema, id_field, time_field=''):
    '''
    Create the table if it does not exist, and pull list of IDs already in the table if it does
    INPUT   table: Carto table to check or create (string)
            schema: dictionary of column names and types, used if we are creating the table for the first time (dictionary)
            id_field: name of column that we want to use as a unique ID for this table; this will be used to compare the
                    source data to the our table each time we run the script so that we only have to pull data we
                    haven't previously uploaded (string)
            time_field:  optional, name of column that will store datetime information (string)
    RETURN  list of existing IDs in the table, pulled from the id_field column (list of strings)
    '''
    # check it the table already exists in Carto
    if cartosql.tableExists(table, user=CARTO_USER, key=CARTO_KEY):
        # if the table does exist, get a list of all the values in the id_field column
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv', post=True, user=CARTO_USER, key=CARTO_KEY)
        # turn the response into a list of strings, removing the first and last entries (header and an empty space at end)
        return r.text.split('\r\n')[1:-1]
    else:
        # if the table does not exist, create it with columns based on the schema input
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema, user=CARTO_USER, key=CARTO_KEY)
        # if a unique ID field is specified, set it as a unique index in the Carto table; when you upload data, Carto
        # will ensure no two rows have the same entry in this column and return an error if you try to upload a row with
        # a duplicate unique ID
        if id_field:
            cartosql.createIndex(table, id_field, unique=True, user=CARTO_USER, key=CARTO_KEY)
        # if a time_field is specified, set it as an index in the Carto table; this is not a unique index
        if time_field:
            cartosql.createIndex(table, time_field, user=CARTO_USER, key=CARTO_KEY)
        # return an empty list because there are no IDs in the new table yet
        return []

'''
FUNCTIONS FOR THIS DATASET

The functions below have been tailored to this specific dataset.
They should all be checked because their format likely will need to be changed.
'''
def tryRetrieveData(url, timeout=300):
    ''' 
    Download data from the source
    INPUT   url: source url to download data (string)
            timeout: how many seconds we will wait to get the data from url (integer) 
    RETURN  res_rows: list of lines in the source data file (list of strings)
    '''  
    # set the start time as the current time so that we can time how long it takes to pull the data (returns the number of seconds passed since epoch)
    start = time.time()
    # elapsed time is initialized with zero
    elapsed = 0

    # try to fetch data from the url while elapsed time is less than the allowed time
    while elapsed < timeout:
        # measures the elapsed time since start
        elapsed = time.time() - start
        try:
            with urllib.request.urlopen(url) as f:
                soup = BeautifulSoup(f)
                tableconf = soup.find( "table", {"id":"conflict"} )
                res_rows = tableconf.find_all('tr')[1:]
                return(res_rows)
        except:
            logging.error("Unable to retrieve resource on this attempt.")
            # if the request fails, wait 5 seconds before moving on to the next attempt to fetch the data
            time.sleep(5)
    # after failing to fetch data within the allowed time, log that the data could not be fetched
    logging.error("Unable to retrive resource before timeout of {} seconds".format(timeout))

    return([])

def processData(url):
    '''
    Fetch, process and upload new data
    INPUT   url: url where you can find the download link for the source data (string)
    RETURN  num_new: number of rows of data sent to Carto table (integer)
    '''
    # initialize variable to store number of new rows sent to Carto
    num_new = 0
    # get the data from source as a list of strings, with each string holding one line from the source data file
    res_rows = tryRetrieveData(url)
    # create a dataframe from the rows
    data = pd.DataFrame([[x.get_text() for x in row.find_all('td')] for row in res_rows], columns = ['date', 'headline', 'conflict_type', 'region', 'description','source', 'latitude', 'longitude', 'start_year', 'end_year'])
    # remove duplicated rows 
    data.drop_duplicates(subset=['date', 'conflict_type', 'region', 'description','sources', 'latitude', 'longitude', 'start_year', 'end_year'], inplace = True, keep='last')
    # create a 'uid' column to store the index of rows as unique ids
    data['uid'] = data.index
    # convert the start years to datetime objects and store them in a new column 'start_dt'
    data['start_dt'] = [datetime.datetime(int(x), 1, 1) if int(x) > 1 else None for x in data.start_year]
    # convert the end years to datetime objects and store them in a new column 'end_dt'
    data['end_dt'] = [datetime.datetime(int(x), 1, 1) if int(x) > 1 else None for x in data.end_year]
    # create 'the_geom' column to store the geometry of the data points
    data['the_geom'] = [{'type': 'Point','coordinates': [x, y]} for (x, y) in zip(data['longitude'], data['latitude'])]
    # reorder the columns in the dataframe 
    data = data[CARTO_SCHEMA.keys()]
    # if there is data available to process
    if len(data):
        num_new = len(data)
        # create a list of new data
        data = data.values.tolist()
        # insert new data into the carto table
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), data, user=CARTO_USER, key=CARTO_KEY)

    return(num_new)

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

def updateResourceWatch(num_new):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   num_new: number of new rows in Carto table (integer)
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = get_most_recent_date(CARTO_TABLE)
        lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)
    
    # Change privacy of table on Carto
    #set up carto authentication using local variables for username (CARTO_WRI_RW_USER) and API key (CARTO_WRI_RW_KEY)
    auth_client = APIKeyAuthClient(api_key=CARTO_KEY, base_url="https://{user}.carto.com/".format(user=CARTO_USER))
    #set up dataset manager with authentication
    dataset_manager = DatasetManager(auth_client)

    # set dataset privacy
    dataset = dataset_manager.get(CARTO_TABLE)
    dataset.privacy = 'LINK'
    dataset.save()
     
    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info("clearing table")
        cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
        # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
        # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
        # problem does not occur

    # Fetch, process, and upload new data
    logging.info('Fetching new data')
    num_new = processData(SOURCE_URL)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    #updateResourceWatch(num_new)

    logging.info("SUCCESS")
