import pandas as pd
import requests
import logging
import os
import cartoframes
import datetime
import cartosql
from collections import OrderedDict
import numpy as np
import sys

'''
Useful links:
http://transitfeeds.com/api/
https://developers.google.com/transit/gtfs/reference/#pathwaystxt
'''

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cit_041_gtfs'

# column of table that can be used as an unique ID (UID)
UID_FIELD = 'feed_id'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('feed_id', 'numeric'),
    ('feed_type', 'text'),
    ('feed_title', 'text'),
    ('loc_id', 'numeric'),
    ('ploc_id', 'numeric'),
    ('loc_title_l', 'text'),
    ('loc_title_s', 'text'),
    ('latitude', 'numeric'),
    ('longitude', 'numeric'),
    ('timestamp_epoch', 'numeric'),
    ('ts_latest', 'timestamp'),
    ('gtfs_zip', 'text'),
    ('gtfs_txt', 'text')
])

# url for locations that provide transit feed data
DATA_LOCATION_URL = 'https://api.transitfeeds.com/v1/getLocations?key=258e3d67-9c2e-46db-9484-001ce6ff3cc7'

# url for transit feed data for each location
DATA_URL = 'https://api.transitfeeds.com/v1/getFeeds?key=258e3d67-9c2e-46db-9484-001ce6ff3cc7&location={}'

# Filename for local files
FILENAME = 'gtfs_points'

# format of dates in source url
INPUT_DATE_FORMAT = '%Y%m%d'

# format of dates in Carto table
DATE_FORMAT = '%Y-%m-%d'

# maximum attempt that will be made to download the data
MAX_TRIES = 8

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'ca607a0d-3ab9-4b22-b4fe-5c43b17e47c4'

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

def getGeom(lon, lat):
    '''
    Define point geometry from latitude and longitude
    INPUT   lon: longitude for point location (float)
            lat: latitude for point location (float)
    RETURN  geojson for point location (geojson)
    '''
    # construct geojson using values from lon, lat columns
    geometry = {
        'type': 'Point',
        'coordinates': [float(lon), float(lat)]
    }
    return geometry

def convert_time_since_epoch(timestamp):
    '''
    Function to convert seconds since the epoch to human-readable time
    INPUT   timestamp: date, formatted as time since linux epoch (integer)
    RETURN  formatted string of date (string)
    '''
    # create a datetime object from timestamp
    value = datetime.datetime.fromtimestamp(timestamp)

    return value.strftime('%Y-%m-%d')


def location():
    '''
    Function to grab the unique location id from the locations api.
    RETURN  list of location ids (list of integers)
    '''
    logging.info('Fetching location ids')
    # get the transit feed data from the url through a request response json
    r = requests.get(DATA_LOCATION_URL)
    json_obj = r.json()
    # store the values from the 'results' feature to a variable
    json_obj_list = json_obj['results']
    # get the values from the 'locations' variable of the 'results' feature 
    json_obj_list_get = json_obj_list.get('locations')
    # create an empty list to store unique ids of each locations
    location_id = []
    # loop through each location
    for dict in json_obj_list_get:
        # get the 'id' from each location
        x = dict.get('id')
        # add the id to the list of unique ids
        location_id.append(x)
    logging.info('Location Ids Collected')

    return location_id


def feeds():
    '''
    Function to use API location ids to obtain the feed information and put them into a 
    pandas dataframe with all the levels of the json unpacked
    RETURN  df: dataframe of transit feed data for all locations (pandas dataframe)
    '''
    # create an empty list to store feed results
    feed_list = []
    logging.info('Fetching Feed info')
    # loop through each locations in the transit feed data using 'id' variable from the JSON
    for id in location():
        # generate url using id and get the data for this location
        r = requests.get(DATA_URL.format(id))
        json_obj = r.json()
        # store 'results' feature from the JSON to a list
        feed_results = json_obj['results']
        # store 'feeds' variable from 'result' feature to a list
        feed_feeds = feed_results['feeds']
        # append the data for this location to the feed_list, if any data is in the list
        try:
            feed_list.append(feed_feeds[0])
        except:
            continue
    # create a pandas dataframe using feed_list
    df_3 = pd.DataFrame(feed_list)
    # There are some columns in the dataframe which are dictionary rather than one dimensional array
    # we want to break the elements from those dictionary and add them as separate columns in the dataframe
    # get the elements from the column 'l' and append them as columns to the end of the dataframe
    # then drop the column 'l' since it's not useful anymore
    df_2 = pd.concat([df_3.drop(['l'], axis=1), df_3['l'].apply(pd.Series)], axis=1)
    # similarly get the elements from the column 'latest' and then drop the column
    df_1 = pd.concat([df_2.drop(['latest'], axis=1), df_2['latest'].apply(pd.Series)], axis=1)
    # similarly get the elements from the column 'u' and then drop the column
    df = pd.concat([df_1.drop(['u'], axis=1), df_1['u'].apply(pd.Series)], axis=1)
    # drop columns where all values are missing
    df = df.dropna(axis=1, how='all')
    # Original columns = 'id', 'ty', 't', 'id', 'pid', 't', 'n', 'lat', 'lng', 0, 'ts', 'd', 'i', 
    # described in API documentation http://transitfeeds.com/api/swagger/#!/default/getFeeds
    # rename the columns to be more descriptive
    new_columns = ['feed_id', 'feed_type', 'feed_title', 'loc_id', 'ploc_id', 'loc_title_l', 'loc_title_s', 'latitude',
                   'longitude', 'timestamp_epoch', 'gtfs_zip', 'gtfs_txt']
    df.columns = new_columns
    # add a new column for geometry using the coulmns 'latitude' and'longitude'
    df['the_geom'] = df.apply(lambda row: getGeom(row['longitude'], row['latitude']), axis=1)
    # replace NaN with zero and infinity with large finite numbers for 'timestamp_epoch' column
    df['timestamp_epoch'] = np.nan_to_num(df['timestamp_epoch'].values)
    # create date string from 'timestamp_epoch' column and add the values to a new column
    df['ts_latest'] = [convert_time_since_epoch(x) for x in df['timestamp_epoch'].values]

    return df


def processData():
    '''
    Function to download data and upload it to Carto.
    We will first try to get the data for MAX_TRIES then quit
    '''
    # set success to False initially
    success = False
    # initialize tries count as 0
    tries = 0
    # create an empty variable to store pandas dataframe
    df = None
    # try to get the data from the url for MAX_TRIES 
    while tries < MAX_TRIES and success == False:
        logging.info('Try running feeds, try number = {}'.format(tries))
        try:
            # pull transit feed data from all locations and format the data into a pandas dataframe
            df = feeds()
            # set success as True after retrieving the data to break out of this loop
            success = True
        except Exception as inst:
            logging.info(inst)
            logging.info("Error fetching data trying again")
            tries = tries + 1
            if tries == MAX_TRIES:
                logging.error("Error fetching data, and max tries reached. See source for last data update.")
    # if we suceessfully collected data from the url
    if success == True:
        # check it the table doesn't already exist in Carto
        if not cartosql.tableExists(CARTO_TABLE, user=CARTO_USER, key=CARTO_KEY):
            logging.info('Table {} does not exist'.format(CARTO_TABLE))
            # if the table does not exist, create it with columns based on the schema input
            cartosql.createTable(CARTO_TABLE, CARTO_SCHEMA)
            # Send dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(df, CARTO_TABLE, overwrite=True, privacy='link')
        else:
            # if the table already exists, delete all the rows
            cartosql.deleteRows(CARTO_TABLE, 'cartodb_id IS NOT NULL', user=CARTO_USER, key=CARTO_KEY)
            # Send the processed dataframe to Carto
            logging.info('Writing to Carto')
            cc = cartoframes.CartoContext(base_url="https://{user}.carto.com/".format(user=CARTO_USER),
                                          api_key=CARTO_KEY)
            cc.write(df, CARTO_TABLE, overwrite=True, privacy='link')

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Fetch, process, and upload new data
    processData()

    # Update dataset's last update date on Resource Watch
    lastUpdateDate(DATASET_ID, datetime.datetime.now())

    logging.info('SUCCESS')
