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
CARTO_TABLE = 'soc_016_conflict_protest_events'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'data_id'

# column that stores datetime information
TIME_FIELD = 'event_date'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ("the_geom", "geometry"),
    ("data_id", "int"),
    ("event_date", "timestamp"),
    ("year", "int"),
    ("time_precision", "int"),
    ("event_type", "text"),
    ("sub_event_type", "text"),
    ("actor1", "text"),
    ("assoc_actor_1", "text"),
    ("inter1", "int"),
    ("actor2", "text"),
    ("assoc_actor_2", "text"),
    ("inter2", "int"),
    ("interaction", "int"),
    ("country", "text"),
    ("iso3", "text"),
    ("region", "text"),
    ("admin1", "text"),
    ("admin2", "text"),
    ("admin3", "text"),
    ("location", "text"),
    ("geo_precision", "int"),
    ("time_precision", "int"),
    ("source", "text"),
    ("source_scale", "text"),
    ("notes", "text"),
    ("fatalities", "int"),
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 10000000

# url for armed conflict location & event data
SOURCE_URL = 'https://api.acleddata.com/acled/read?terms=accept&page={page}'

# minimum pages to process
MIN_PAGES = 15

# maximum pages to process
MAX_PAGES = 400

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'ea208a8b-4559-434b-82ee-95e041596a3a'

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

def processNewData(src_url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   src_url: unformatted url where you can find the source data (string)
            existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  new_ids: list of unique ids of new data sent to Carto table (list of strings)
    '''
    # specify the page of source url we want to pull
    # initialize at 0 so that we can start pulling from page 1 in the loop
    page = 0
    # length (number of rows) of new_data
    # initialize at 1 so that the while loop works during first step
    new_count = 1
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []

    # get and parse each page; stop when no new results or max pages
    # process upto MIN_PAGES even if there are no new results from them
    while page <= MIN_PAGES or new_count and page < MAX_PAGES:
        try:
            # increment page number in every loop
            page += 1
            logging.info("Fetching page {}".format(page))
            # generate the url and pull data for this page 
            r = requests.get(src_url.format(page=page))
            # pull data from request response json
            data = r.json()
            # create an empty list to store each row of new data
            new_rows = []
            # loop until no new observations
            for obs in data['data']:
                # generate unique id by using the data_id feature from json
                uid = str(obs[UID_FIELD])
                # if the id doesn't already exist in Carto table or 
                # isn't added to the list for sending to Carto yet 
                if uid not in existing_ids + new_ids:
                    # append the id to the list for sending to Carto 
                    new_ids.append(uid)
                    # create an empty list to store data from this row
                    row = []
                    # go through each column in the Carto table
                    for field in CARTO_SCHEMA.keys():
                        # if we are fetching data for geometry column
                        if field == 'the_geom':
                            # construct geojson geometry
                            geom = {
                                "type": "Point",
                                "coordinates": [
                                    obs['longitude'],
                                    obs['latitude']
                                ]
                            }
                            # add geojson geometry to the list of data from this row
                            row.append(geom)
                        # if we are fetching data for unique id column
                        elif field == UID_FIELD:
                            # add the unique id to the list of data from this row
                            row.append(uid)
                        else:
                            try:
                                # add data for remaining fields to the list of data from this row
                                row.append(obs[field])
                            except:
                                logging.debug('{} not available for this row'.format(field))
                                # if the column we are trying to retrieve doesn't exist in the source data, store blank
                                row.append('')
                    # add the list of values from this row to the list of new data
                    new_rows.append(row)

            # find the length (number of rows) of new_data 
            new_count = len(new_rows)
            # check if new data is available
            if new_count:
                logging.info('Pushing {} new rows'.format(new_count))
                # insert new data into the carto table
                cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                    CARTO_SCHEMA.values(), new_rows, user=CARTO_USER, key=CARTO_KEY)
        except:
            logging.error('Could not fetch or process page {}'.format(page))

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
