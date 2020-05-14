import os
import logging
import sys
from collections import OrderedDict
import datetime
import cartosql
import requests
import simplejson
import time

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of tables in Carto where we will upload the data
# TABLE_LEGACY is the one currently being implemented for this dataset
# use either TABLE_DAILY or TABLE_3HR in future
TABLE_DAILY = 'dis_012a_landslide_hazard_alerts_daily'
TABLE_3HR = 'dis_012b_landslide_hazard_alerts_3hr'
TABLE_LEGACY = 'dis_012_landslide_hazard_alerts_explore'

# url for landslide hazards data
# example url: https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast_3hr&limit=100000000&startTime=2018-03-27T15:57:59.904895&endTime=2019-03-27T15:59:19.100245
URL_3HR = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast_3hr&limit=100000000&startTime={startTime}&endTime={endTime}'
URL_DAILY = 'https://pmmpublisher.pps.eosdis.nasa.gov/opensearch?q=global_landslide_nowcast&limit=100000000&startTime={startTime}&endTime={endTime}'

# dictionary of tables as keys and corresponding source urls as values
TABLES = {
    TABLE_DAILY: URL_DAILY,
    TABLE_3HR: URL_3HR,
    TABLE_LEGACY: URL_3HR
}

# column of table that can be used as a unique ID (UID)
UID_FIELD = '_UID'

# column that stores datetime information
TIME_FIELD = 'datetime'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('_UID', 'text'),
    ('datetime', 'timestamp'),
    ('nowcast', 'numeric'),
    ('the_geom', 'geometry')
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 100000

# oldest date that can be stored in the Carto table before we start deleting
MAX_AGE = datetime.datetime.utcnow() - datetime.timedelta(days=365)

# if we get error during data fetching process, how many seconds do you want to wait before you try to fetch again?
# currently, when fetching process fails, the 5 tries take 10 mins, so we will try with a wait time of 15 mins
WAIT_TIME = 15*60

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '444138cd-8ef4-48b3-b197-73e324175ad0'

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

def genUID(datetime, position_in_geojson):
    '''Generate unique id using date and index in retrieved json
    INPUT   datetime: date for which we want to generate id (string)
            position_in_geojson: index of the datetime in json (string)
    '''
    return '{}_{}'.format(datetime, position_in_geojson)

def processData(src_url, table, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   src_url: url where you can find the download link for the source data (string)
            table: name of table in Carto where we will upload the data (string)
            existing_ids: list of date IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # set the start date for collecting data to oldest date of data stored in the Carto
    # table in the format (YYYY-MM-DD)
    start_time = MAX_AGE.isoformat()
    # set the start date for collecting data to current time in the format (YYYY-MM-DD)
    end_time = datetime.datetime.utcnow().isoformat()

    # set try number to 1 because this will be our first try fetching the data
    try_num = 1
    # try at least 5 times to fetch the data for this area from the source
    while try_num <= 5:
        try:
            logging.info('Pulling data from source, try number %s' %try_num)
            # generate the url and pull data for the selected interval
            r = requests.get(src_url.format(startTime=start_time, endTime=end_time))
            # pull data from request response json
            results = r.json()
            break
        except simplejson.errors.JSONDecodeError:
            logging.info('Waiting for {} seconds before trying again.'.format(WAIT_TIME))
            # if we get error during data fetching process, 
            # we wait for an interval based on the variable WAIT_TIME and then try to fetch again
            time.sleep(WAIT_TIME)
            try_num +=1

    # loop until no new observations
    for item in results['items']:
        # create an empty list to store each rows of new data
        new_rows = []

        # get date and url from each item in the results json
        date = item['properties']['date']['@value']
        url = item['action'][5]['using'][0]['url']

        logging.info('Fetching data for {}'.format(date))
        # pull data from url using request response json
        data = requests.get(url).json()

        # loop through to retrieve data from each geojson features
        for i in range(len(data['features'])):
            # generate unique id for this data
            uid = genUID(date, i)
            # if the id doesn't already exist in Carto table or 
            # isn't added to the list for sending to Carto yet
            if uid not in existing_ids and uid not in new_ids:
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # get features for this index
                obs = data['features'][i]
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA:
                    # if we are fetching data for unique id column
                    if field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for datetime column
                    if field == TIME_FIELD:
                        # add the datetime information to the list of data from this row
                        row.append(date)
                    # if we are fetching data for landslide hazard alert column
                    if field == 'nowcast':
                        # add the landslide hazard alert information to the list of data from this row
                        row.append(obs['properties']['nowcast'])
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # add the geometry information to the list of data from this row
                        row.append(obs['geometry'])
                # add the list of values from this row to the list of new data
                new_rows.append(row)

        # find the length (number of rows) of new_data 
        num_new = len(new_rows)
        # check if length of new data is less than the maximum allowable data on Carto
        if num_new and len(new_ids) < MAX_ROWS:
            logging.info("Inserting {} new rows".format(num_new))
            # insert new data into the carto table
            cartosql.insertRows(table, CARTO_SCHEMA.keys(),CARTO_SCHEMA.values(), new_rows, user=CARTO_USER, key=CARTO_KEY)
        else:
            break

    return new_ids

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age: oldest date that can be stored in the Carto table (datetime object)
    RETURN  num_dropped: number of rows that have been dropped from the table (integer)
    ''' 
    # initialize number of rows that will be dropped as 0
    num_dropped = 0

    # check if max_age is a datetime object
    if isinstance(max_age, datetime.datetime):
        # convert max_age to a string in the format (YYYY-MM-DD)
        max_age = max_age.isoformat()

    # if the max_age variable exists
    if max_age:
        # delete rows from table which are older than the max_age
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), CARTO_USER, CARTO_KEY)
        # get the number of rows that were dropped from the table
        num_dropped = r.json()['total_rows']

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

def updateResourceWatch(num_new):
    '''
    This function should update Resource Watch to reflect the new data.
    This may include updating the 'last update date' and updating any dates on layers
    INPUT   num_new: number of new rows in Carto table (integer)
    '''
    # If there are new entries in the Carto table
    if num_new>0:
        # Update dataset's last update date on Resource Watch
        most_recent_date = get_most_recent_date(TABLE_LEGACY)
        lastUpdateDate(DATASET_ID, most_recent_date)

    # Update the dates on layer legends - TO BE ADDED IN FUTURE

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # update each tables in Carto for this dataset
    for table, url in TABLES.items():
        logging.info('Processing data for {}'.format(table))
        # Check if table exists, create it if it does not
        existing_ids = checkCreateTable(table, CARTO_SCHEMA, UID_FIELD,TIME_FIELD)

        # Fetch, process, and upload new data
        new_ids = processData(url, table, existing_ids)
        # find the length of new data that were uploaded to Carto
        new_count = len(new_ids)
        logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), new_count))

        # Delete data to get back to MAX_ROWS
        # Sometimes Carto fails to delete, so we will add multiple tries
        try_num = 1
        while try_num <= 5:
            try:
                logging.info('Deleting excess observations.')
                num_deleted = deleteExcessRows(table, MAX_ROWS, TIME_FIELD, MAX_AGE)
                logging.info('Successfully deleted excess rows.')
                break
            except:
                logging.info('Waiting for {} seconds before trying again.'.format(WAIT_TIME))
                # if we get error during deleting process, 
                # we wait for 30 seconds and then try to delete again
                time.sleep(30)
                try_num += 1

    # Update Resource Watch
    updateResourceWatch(new_count)

    logging.info('SUCCESS')
