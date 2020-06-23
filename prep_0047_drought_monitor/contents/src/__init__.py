from __future__ import unicode_literals

import fiona
import os
import logging
import sys
import urllib
import datetime
from collections import OrderedDict
import cartosql
import zipfile
import requests

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# name of data directory in Docker container
DATA_DIR = 'data'

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# url for source data
SOURCE_URL = 'http://droughtmonitor.unl.edu/data/shapefiles_m/USDM_{date}_M.zip'

# Specify filename that will be used to construct source url
FILENAME = 'USDM_{date}'

# Specify how many days we want to go back from today's date to search for data
TIMESTEP = {'days': 1}

# format of dates in Carto table
DATE_FORMAT = '%Y%m%d'

# The weekly maps of U.S. Drought Monitor are released each Thursday and are assessments 
# of past conditions based on data through the preceding Tuesday.
# We want to process Tuesday, and Tuesday is represented by 1 in the date.weekday() function 
WEEKDAY = 1

# name of table in Carto where we will upload the data
CARTO_TABLE = 'prep_0047_drought_monitor'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('date', 'timestamp'),
    ('OBJECTID', 'int'),
    ('DM', 'int')
])

# column of table that can be used as an unique ID (UID)
UID_FIELD = '_UID'

# column that stores datetime information
TIME_FIELD = 'date'

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAXROWS = 10000

# oldest date that can be stored in the Carto table before we start deleting
MAXAGE = datetime.datetime.today() - datetime.timedelta(days=365*10)

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = 'ddf88c85-3e2f-41fa-8ceb-a3633ffb0bfb'

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

def genUID(obs, date):
    '''
    Generate unique id using date and OBJECTID variable
    INPUT   date: date for which we want to generate id (string)
            obs: features from the GeoJSON (GeoJSON feature)
    RETURN unique id for row (string)
    '''
    return str('{}_{}'.format(date, obs['properties']['OBJECTID']))


def getDate(uid):
    '''
    Get date from first eight characters of the unique id
    INPUT   uid: unique ID for Carto table (string)
    RETURN  date from the unique ID (integer)
    '''
    return uid[:8]


def findShp(zfile):
    '''
    Check if the input zipfile contains a shapefile and return the shapefile
    INPUT   zfile: zipfile containing retrieved data from source url (list of strings)
    RETURN  shapefile: name of the shapefile (string)
    '''
    # loop through all files in the zipped file
    with zipfile.ZipFile(zfile) as z:
        for shapefile in z.namelist():
            # check if the file is a shapefile
            if os.path.splitext(shapefile)[1] == '.shp':
                # return the name of the shapefile
                return shapefile
    return False


def getNewDates(exclude_dates):
    '''
    Get new dates that we need to process
    INPUT   exclude_dates: dates that we already have in our Carto table (list of strings)
    RETURN  new_dates: new dates that we want to process (list of strings)
    '''
    # create an empty list to store new dates
    new_dates = []
    # get the start date by going back number of days set by the "TIMESTEP" variable
    date = datetime.datetime.today() - datetime.timedelta(**TIMESTEP)
    # Get new dates; continue until the current date is older than the oldest date
    # allowed in the table, set by the MAX_AGE variable
    while date > MAXAGE:
        # iterate backwards by going back number of days set by the "TIMESTEP" variable
        date -= datetime.timedelta(**TIMESTEP)
        # Check if the day is equal to the day set by the WEEKDAY variable. date.weekday() function 
        # returns the day of the week as an integer, where Monday is 0 and Sunday is 6.
        if date.weekday() == WEEKDAY:
            # convert the datetime to string in the format set by DATE_FORMAT variable
            datestr = date.strftime(DATE_FORMAT)
            # if the date doesn't already exist in our Carto table
            if datestr not in exclude_dates:
                # add the date to the list of new dates
                new_dates.append(datestr)

    return new_dates


def processNewData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []

    # get dates that already exist in Carto table
    dates = set([getDate(uid) for uid in existing_ids])
    # get new dates that we want to process
    new_dates = getNewDates(dates)
    # loop through each new date
    for date in new_dates:
        # generate url to pull data for this date
        url = SOURCE_URL.format(date=date)
        # create file name to store the data from the source url
        tmpfile = '{}.zip'.format(os.path.join(DATA_DIR,
                                               FILENAME.format(date=date)))
        logging.info('Fetching {}'.format(date))
        try:
            # pull data from url and save to tmpfile
            urllib.request.urlretrieve(url, tmpfile)
        except Exception as e:
            logging.warning('Could not retrieve {}'.format(url))
            logging.error(e)
            # skip dates that don't work
            continue

        # parse fetched data and generate unique ids
        logging.info('Parsing data')
        # check if the tmpfile contain the expected shapefile
        # format path for the shapefile
        shpfile = '/{}'.format(findShp(tmpfile))
        # format path for the zipfiles
        zfile = 'zip://{}'.format(tmpfile)
        # create an empty list to store each row of new data
        rows = []
        # open the shapefile as GeoJSON and process it
        with fiona.open(shpfile, 'r', vfs=zfile) as shp:
            logging.debug(shp.schema)
            # loop through each features in the GeoJSON
            for obs in shp:
                # generate unique id using date and feature information
                uid = genUID(obs, date)
                # append the id to the list for sending to Carto 
                new_ids.append(uid)
                # create an empty list to store data from this row
                row = []
                # go through each column in the Carto table
                for field in CARTO_SCHEMA.keys():
                    # if we are fetching data for geometry column
                    if field == 'the_geom':
                        # get geometry from the 'geometry' feature in GeoJSON,
                        # add geometry to the list of data from this row
                        row.append(obs['geometry'])
                    # if we are fetching data for unique id column
                    elif field == UID_FIELD:
                        # add the unique id to the list of data from this row
                        row.append(uid)
                    # if we are fetching data for datetime column
                    elif field == TIME_FIELD:
                        # add datetime information to the list of data from this row
                        row.append(date)
                    else:
                        # add data for remaining fields to the list of data from this row
                        row.append(obs['properties'][field])
                # add the list of values from this row to the list of new data
                rows.append(row)
        # delete local files
        os.remove(tmpfile)

        # find the length (number of rows) of new_data 
        new_count = len(rows)
        # check if new data is available
        if new_count:
            logging.info('Pushing new rows')
            # insert new data into the carto table
            cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                CARTO_SCHEMA.values(), rows, user=CARTO_USER, key=CARTO_KEY)

    # length (number of rows) of new_data 
    num_new = len(new_ids)

    return num_new


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    ''' 
    Delete rows that are older than a certain threshold and also bring count down to max_rows
    INPUT   table: name of table in Carto from which we will delete excess rows (string)
            max_rows: maximum rows that can be stored in the Carto table (integer)
            time_field: column that stores datetime information (string) 
            max_age(optional): oldest date that can be stored in the Carto table (datetime object)
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
    num_new = processNewData(existing_ids)

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info('SUCCESS')
