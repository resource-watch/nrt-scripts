import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'cit_004_city_aq'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("uid", "text"),
    ("name", "text"),
    ("city", "text"),
    ("location", "text"),
    ("created", "timestamp"),
    ("date", "timestamp"),
    ("o3", "numeric"),
    ("no2", "numeric"),
    ("pm25_gcc", "numeric"),
    ("pm25_gocart", "numeric"),
])

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'date'

# format of dates in source and Carto table
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 10000000

# url for devseed air quality data
SOURCE_URL = 'http://gmao-aq-staging-1504401194.us-east-1.elb.amazonaws.com/api/forecast/'

# url to get info about specific station
STATION_URL = 'http://gmao-aq-staging-1504401194.us-east-1.elb.amazonaws.com/api/station/{station}/'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'f5599d62-7f3d-41c7-b3fd-9f8e08ee7b2a'

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

def genUID(dt, stn):
    '''Generate unique id using date and station number
    INPUT   dt: date for which we want to generate id (string)
            stn: station number (integer)
    RETURN unique id for row (string)
    '''
    # split the date on '-' to create separate pieces
    dt_pieces = dt.split('-')
    # join the pieces to have date with format YYMMDDHH(2020-07-04T12:00:00.000Z > 20200704T12)
    mod_dt = dt_pieces[0] + dt_pieces[1] + dt_pieces[2].split(':')[0]
    # joint the formatted date with station number to generate the unique id
    return '{}_{}'.format(mod_dt, stn)

def processNewData(src_url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   src_url: url where you can find the source data (string)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    # get data from source url
    r = requests.get(src_url)
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # create an empty list to store each row of new data
    new_rows = []
    # pull data from request response json
    data = r.json()

    # loop until no new observations
    for obs in data:
        # get the date from date feature 
        dt = obs['date']
        # get the station number from 'station' feature
        stn = obs['station']
        # generate unique id by using the date and station number
        uid = genUID(dt, stn)
        # if the id doesn't already exist in Carto table or 
        # isn't added to the list for sending to Carto yet 
        if uid not in existing_ids + new_ids:
            # append the id to the list for sending to Carto 
            new_ids.append(uid)
            # create an empty list to store data from this row
            row = []
            # go through each column in the Carto table
            for field in CARTO_SCHEMA.keys():
                # generate url to get details of the station being processed
                stn_url = STATION_URL.format(station = stn)
                # get data from station url
                stn_r = requests.get(stn_url)
                # pull data from request response json
                stn_data = stn_r.json()

                # if we are fetching data for geometry column
                if field == 'the_geom':
                    # construct geojson geometry
                    geom = stn_data.get("geometry")
                    # add geojson geometry to the list of data from this row
                    row.append(geom)
                # if we are fetching data for unique id column
                elif field == 'uid':
                    # add already generated unique id to the list of data from this row
                    row.append(uid)
                # if we are fetching data for station name
                elif field == 'name':
                    # get station name from the dictionary
                    name = stn_data.get("properties").get("name")
                    # add text to the list of data from this row
                    row.append(name)
                elif field == 'city':
                    # get city name from the station name
                    city = stn_data.get("properties").get("name").split('_')[0]
                    # add text to the list of data from this row
                    row.append(city)
                elif field == 'location':
                    # get station name from the station name
                    location = stn_data.get("properties").get("name").split('_')[1]
                    # add text to the list of data from this row
                    row.append(location)
                # if we are fetching data for date of forecast creation column
                elif field == 'created':
                    # turn already generated date into a datetime
                    date = datetime.datetime.strptime(obs['created'], DATETIME_FORMAT)
                    # add date to the list of data from this row
                    row.append(date)
                # if we are fetching data for date column
                elif field == 'date':
                    # turn already generated date into a datetime
                    date = datetime.datetime.strptime(dt, DATETIME_FORMAT)
                    # add date to the list of data from this row
                    row.append(date)
                # remaining fields to process are the different air quality variables
                else:
                    # add data for remaining fields to the list of data from this row
                    row.append(obs['gas'].get(field))

            # add the list of values from this row to the list of new data
            new_rows.append(row)
    # find the length (number of rows) of new_data 
    new_count = len(new_rows)
    # check if new data is available
    if new_count:
        logging.info('Pushing {} new rows'.format(new_count))
        # insert new data into the carto table
        cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(),
                             new_rows, user=CARTO_USER, key=CARTO_KEY)
    return new_ids

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: optional, oldest date that can be stored in the Carto table (datetime object)
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0

    # check if max_age is a datetime object
    if isinstance(max_age, datetime.datetime):
        # convert max_age to a string 
        max_age = max_age.isoformat()

    # if the max_age variable exists
    if max_age:
        # delete rows from table which are older than the max_age
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), user=CARTO_USER, key=CARTO_KEY)
        # get the number of rows that were dropped from the table
        num_dropped = r.json()['total_rows']

    # get cartodb_ids from carto table sorted by date (new->old)
    r = cartosql.getFields('cartodb_id', table, order='{} desc'.format(time_field),
                           f='csv', user=CARTO_USER, key=CARTO_KEY)
    # turn response into a list of strings of the ids
    ids = r.text.split('\r\n')[1:-1]

    # if number of rows is greater than max_rows, delete excess rows
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[max_rows:], user=CARTO_USER, key=CARTO_KEY)
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
    # get current datetime
    now = datetime.datetime.utcnow()
    # if most_recent_date is newer than current datetime
    if most_recent_date > now:
        # change most_recent_date to current datetime
        most_recent_date = now

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
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD, TIME_FIELD)

    # Fetch, process, and upload new data
    new_ids = processNewData(SOURCE_URL, existing_ids)
    # find the length of new data that were uploaded to Carto
    num_new = len(new_ids)

    # Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')