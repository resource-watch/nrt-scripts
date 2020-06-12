import logging
import sys
import os

import requests
from collections import OrderedDict
import datetime
import cartosql
import csv

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'ene_008_us_oil_chemical_spills'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('uid', 'numeric'),
    ('the_geom', 'geometry'),
    ('open_date', 'timestamp'),
    ('name', 'text'),
    ('location', 'text'),
    ('threat', 'text'),
    ('tags', 'text'),
    ('commodity', 'text'),
    ('measure_skim', 'numeric'),
    ('measure_shore', 'numeric'),
    ('measure_bio', 'numeric'),
    ('measure_disperse', 'numeric'),
    ('measure_burn', 'numeric'),
    ('max_ptl_release_gallons', 'numeric'),
    ('posts', 'numeric'),
    ('description', 'text')
])

# column of table that can be used as an unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'open_date'

# url for oil and chemical spill incidents data
SOURCE_URL = "https://incidentnews.noaa.gov/raw/incidents.csv"

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '8746e75d-2749-405e-8f3b-0c12097860a1'

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

def processData(existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   existing_ids: list of unique IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''

    # create an empty list to store each row of new data
    new_rows = []
    # pull data from the url 
    res = requests.get(SOURCE_URL)
    # read the contents of the url as a csv file
    # return a reader object which will iterate over lines in the given csvfile
    # https://stackoverflow.com/questions/18897029/read-csv-file-from-url-into-python-3-x-csv-error-iterator-should-return-str
    csv_reader = csv.reader(res.iter_lines(decode_unicode=True))
    # get headers from the csv file
    headers = next(csv_reader, None)
    # loop through each column and store column names and values to an empty dictionary 
    idx = {k: v for v, k in enumerate(headers)}
    # iterate over each line in the reader object
    for row in csv_reader:
        # skip empty rows
        if not len(row):
            continue
        else:
            # This data set has some entries with breaks in the last column, which the csv_reader interprets
            # as an individual row. See if new id can be converted to an integer. If it can, it is probably a
            # new row.
            try:
                # check if new id can be converted to an integer
                int(row[idx['id']])
                # generate unique id from the 'id' column
                id = row[idx['id']]
                # if the id doesn't already exist in Carto table
                if id not in existing_ids:
                    logging.info('new row for {}'.format(id))
                    # create an empty list to store data from this row
                    new_row = []
                    # go through each column in the Carto table
                    for field in CARTO_SCHEMA:
                        # if we are fetching data for unique id column
                        if field == 'uid':
                            # add the unique id to the list of data from this row
                            new_row.append(row[idx['id']])
                        # if we are fetching data for geometry column
                        elif field == 'the_geom':
                            # Check for whether valid lat lon provided, will fail if either are ''(empty)
                            # get longitude from the column 'lon'
                            lon = row[idx['lon']]
                            # get latitude from the column 'lat'
                            lat = row[idx['lat']]
                            # if both latitude and longitude are present
                            if lat and lon:
                                # construct geojson geometry with lat, lon
                                geometry = {
                                    'type': 'Point',
                                    'coordinates': [float(lon), float(lat)]
                                }
                                # add geojson geometry to the list of data from this row
                                new_row.append(geometry)
                            else:
                                # log if no lat lon available and store None for geometry
                                logging.debug('No lat long available for this data point - skipping!')
                                new_row.append(None)
                        else:
                            try:
                                # for all other columns, we can fetch the data using our column name in Carto
                                # if the column we are trying to retrieve doesn't exist in the source data, store None
                                val = row[idx[field]] if row[idx[field]] != '' else None
                                new_row.append(val)
                            except IndexError:
                                pass
                    # add the list of values from this row to the list of new data
                    new_rows.append(new_row)
            # If we could't convert id to an integer, the last row probably got cut off.
            except ValueError:
                # Using the id from the last entry, if this id was already in the Carto table, we will skip it
                if id in existing_ids:
                    pass
                # If it is a new id, we need to go fix that row.
                else:
                    # If the row is only one item, append the rest of the information to the last description.
                    if len(row) == 1:
                        new_rows[-1][-1] = new_rows[-1][-1] + ' ' + row[0].replace('\t', '')
                    # If several things are in the row, the break was probably mid-row.
                    elif len(row) > 1 and len(row) < 17:
                        # finish the last description of the last row in the list
                        new_rows[-1][-1] = new_rows[-1][-1] + ' ' + row[0].replace('\t', '')
                        # append other items to row
                        new_row = new_rows[-1]
                        # need to process one less column for last row since description is already filled
                        offset_factor = len(new_rows[-1])-1
                        # go through each column in the Carto table
                        for field in CARTO_SCHEMA:
                            # skip if the field is 'uid' or 'the_geom'
                            if field == 'uid' or field == 'the_geom':
                                continue
                            try:
                                # find the appropriate location for the field by subtracting offset
                                loc=idx[field]-offset_factor
                                if loc>0:
                                    # insert value for columns to the new_row list 
                                    # if the column is empty, insert None
                                    val = row[loc] if row[loc] != '' else None
                                    new_row.append(val)
                            except IndexError:
                                pass
                        # check if last row in new_rows matches with the new_row list
    # find the length (number of rows) of new_data 
    num_new = len(new_rows)
    # check if new data is available
    if num_new:
        logging.info("Inserting {} new rows".format(num_new))
        # insert new data into the carto table
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
                                 CARTO_SCHEMA.values(), new_rows, user=CARTO_USER, key=CARTO_KEY)

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
    num_new = processData(existing_ids)

    # Remove old observations
    deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
