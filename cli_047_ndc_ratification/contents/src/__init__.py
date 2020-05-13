import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests
import pandas as pd

'''
----------------------------------------------------Important Note--------------------------------------------------------
# The indicator id for this dataset changes somewhat regularly, which breaks this script. If no data has been returned for
# the current ID, you will have to go to the source to find the new id.
# 1. Go to Climate Watch Data Explorer: https://www.climatewatchdata.org/data-explorer
# 2. Click to the 'NDC Content' data tab
# 3. Under the 'Indicators' drop down, select 'Status of Ratification'
# 4. At this point, in the url, you should see the parameter id indicated after 'ndc-content-indicators=' - copy this id
#    into the indicator_id variable, below.
----------------------------------------------------Important Note--------------------------------------------------------
'''
indicator_id = 16200

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cli_047_ndc_ratification'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'id'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('id', 'text'),
    ('iso_code3', 'text'),
    ('country', 'text'),
    ('value', 'text'),
])

# url for NDC data
SOURCE_URL = 'https://www.climatewatchdata.org/api/v1/data/ndc_content?indicator_ids[]=%s&page={page}'%indicator_id

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '136aab69-c625-4347-b16a-c2296ee5e99e'

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

def processNewData(url):
    '''
    Fetch, process and upload new data
    INPUT   url: url where you can find the download link for the source data (string)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # specify the starting page of source url we want to pull
    page = 1
    # generate the url and pull data for this page 
    r = requests.get(url.format(page=page))
    # pull data from request response json
    raw_data = r.json()['data']
    # if data is available from source url 
    if len(raw_data)>0:
        # if the table exists
        if cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            # delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
        logging.info('Updating {}'.format(CARTO_TABLE))
    else:
        # raise an error that data is not available from source url 
        logging.error("Source data missing. Table will not update.")
    # create an empty list to store new data
    new_data = []
    # if data is available from source url 
    while len(raw_data)>0:
        logging.info('Processing page {}'.format(page))
        # read in source data as a pandas dataframe
        df = pd.DataFrame(raw_data)
        # go through each rows in the dataframe
        for row_num in range(df.shape[0]):
            # get the row of data
            row = df.iloc[row_num]
            # create an empty list to store data from this row
            new_row = []
            # go through each column in the Carto table
            for field in CARTO_SCHEMA:
                # if we are fetching data for unique id column
                if field == 'uid':
                    # add the unique id to the list of data from this row
                    new_row.append(row[UID_FIELD])
                # for any other column, check if there are values available from the source for this row    
                else:
                    # if data available from source for this field, populate the field with the data
                    # else populate with None
                    val = row[field] if row[field] != '' else None
                    # add this value to the list of data from this row
                    new_row.append(val)
            # add the list of values from this row to the list of new data        
            new_data.append(new_row)
        # go to the next page and check for data
        page += 1
        # generate the url and pull data for this page 
        r = requests.get(url.format(page=page))
        # pull data from request response json
        raw_data = r.json()['data']

    # find the length (number of rows) of new_data    
    num_new = len(new_data)
    # if we have found new dates to process
    if num_new:
        # insert new data into the carto table
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)

    return num_new

def updateResourceWatch(num_new):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   num_new: number of new rows in Carto table (integer)
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = datetime.datetime.utcnow()
        lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # clear the table before starting, if specified
    if CLEAR_TABLE_FIRST:
        logging.info("clearing table")
        # if the table exists
        if cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            # delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # note: we do not delete the entire table because this will cause the dataset visualization on Resource Watch
            # to disappear until we log into Carto and open the table again. If we simply delete all the rows, this
            # problem does not occur

    # Check if table exists, create it if it does not
    logging.info('Checking if table exists and getting existing IDs.')
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD)

    # Fetch, process, and upload new data
    logging.info('Fetching new data')
    num_new = processNewData(SOURCE_URL)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Update Resource Watch
    updateResourceWatch(num_new)
    
    logging.info('SUCCESS')
