import logging
import sys
import os
import requests as req
from collections import OrderedDict
import cartosql
import lxml
from xmljson import parker as xml2json
from dateutil import parser
import requests
import datetime

# do you want to delete everything currently in the Carto table when you run this script?
CLEAR_TABLE_FIRST = False

# Carto username and API key for account where we will store the data
CARTO_USER = os.getenv('CARTO_USER')
CARTO_KEY = os.getenv('CARTO_KEY')

# name of table in Carto where we will upload the data
CARTO_TABLE = 'dis_003_volcano_reports'

# column of table that can be used as a unique ID (UID)
UID_FIELD = 'uid'

# column that stores datetime information
TIME_FIELD = 'pubdate'

# column names and types for data table
# column names should be lowercase
# column types should be one of the following: geometry, text, numeric, timestamp
CARTO_SCHEMA = OrderedDict([
    ('uid', 'text'),
    ('the_geom', 'geometry'),
    ('pubdate', 'timestamp'),
    ('volcano_name', 'text'),
    ('country_name', 'text'),
    ('description', 'text'),
    ('sources', 'text')
])

# how many rows can be stored in the Carto table before the oldest ones are deleted?
MAX_ROWS = 1000000

# oldest date that can be stored in the Carto table before we start deleting
MAX_AGE = datetime.datetime.today() - datetime.timedelta(days=365*5)

# url for USGS Weekly Volcanic Activity Report
SOURCE_URL = "http://volcano.si.edu/news/WeeklyVolcanoRSS.xml"

# format of dates in Carto table
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

# Resource Watch dataset API ID
# Important! Before testing this script:
# Please change this ID OR comment out the getLayerIDs(DATASET_ID) function in the script below
# Failing to do so will overwrite the last update date on a different dataset on Resource Watch
DATASET_ID = '60d3b365-6c0b-4f1c-9b7f-f3f00f2a05d7'

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

def genUID(lat,lon,dt):
    '''Generate unique id using latitude, longitude and date information
    INPUT   lat: latitude of the earthquake (float)
            lon: longitude of the earthquake (float)
            dt: date for which we want to generate id (string)
    RETURN unique id for row (string)
    '''
    return '{}_{}_{}'.format(lat,lon,dt)

def processData(url, existing_ids):
    '''
    Fetch, process and upload new data
    INPUT   url: url where you can find the source data (string)
            existing_ids: list of IDs that we already have in our Carto table (list of strings)
    RETURN  num_new: number of rows of new data sent to Carto table (integer)
    '''
    # create an empty list to store new data
    new_data = []
    # create an empty list to store unique ids of new data we will be sending to Carto table
    new_ids = []
    # pull data from the url
    res = req.get(url)
    # break down the content retrieved from the url
    xml = lxml.etree.fromstring(res.content)
    # convert xml into Python dictionary structure (json in this case)
    json = xml2json.data(xml)
    # get each volcano report from the item feature in json
    items = json['channel']['item']
    # loop through to process each volcano report
    for item in items:
        # get the volcano name and country name from the title 
        title = item['title'].split(')')[0].split('(')
        # remove leading and trailing whitespaces from title
        place_info = [place.strip() for place in title]
        # get the volcano name from first index of place_info
        volcano_name = place_info[0]
        # get the country name from second index of place_info
        country_name = place_info[1]
        # get the coordinates and split it to separate out latitude and longitude
        coords = item['{http://www.georss.org/georss}point'].split(' ')
        # get the longitude from first index of coords
        lat = coords[0]
        # get the latitude from second index of coords
        lon = coords[1]
        # create a geojson from the coordinates
        geom = {
            'type':'Point',
            'coordinates':[lon,lat]
        }
        # get the date from pubDate feature and format according to DATETIME_FORMAT
        dt = parser.parse(item['pubDate'], fuzzy=True).strftime(DATETIME_FORMAT)
        # get the description and source of the volcano report from description feature
        info = item['description'].split('Source:')
        # if the word 'Source:' doesn't exist in this item, it may be because there is more than one source (labeled as 'Sources')
        if len(info) < 2:
            # get the description and sources of the volcano report from description feature 
            info = item['description'].split('Sources:')
        # remove the <p> tags from begining and end of info 
        description_text = [text.replace('<p>','').replace('</p>','') for text in info]
        # get the description of the volcano report from the first index of description_text
        description = description_text[0]
        # get the source/sources of the volcano report from the second index of description_text
        sources = description_text[1]
        # generate unique id by using the coordinates, and datetime of the volcano
        _uid = genUID(lat,lon,dt)
        # if the id doesn't already exist in Carto table or 
        # isn't added to the list for sending to Carto yet 
        if _uid not in existing_ids and _uid not in new_ids:
            # append the id to the list for sending to Carto 
            new_ids.append(_uid)
            # create an empty list to store data from this row
            row = []
            # go through each column in the Carto table
            for field in CARTO_SCHEMA:
                # if we are fetching data for unique id column
                if field == 'uid':
                    # add the unique id to the list of data from this row
                    row.append(_uid)
                # if we are fetching data for geometry column
                elif field == 'the_geom':
                    # add the geometry to the list of data from this row
                    row.append(geom)
                # if we are fetching data for datetime column
                elif field == 'pubdate':
                    # add datetime information to the list of data from this row
                    row.append(dt)
                # if we are fetching data for description column
                elif field == 'description':
                    # add the description of the report to the list of data from this row
                    row.append(description)
                # if we are fetching data for sources column
                elif field == 'sources':
                    # add the source/sources of the report to the list of data from this row
                    row.append(sources)
                # if we are fetching data for volcano_name column
                elif field == 'volcano_name':
                    # add the name of the volcano to the list of data from this row
                    row.append(volcano_name)
                # if we are fetching data for description column
                elif field == 'country_name':
                    # add the name of country where this volcano was reported to the list of data from this row
                    row.append(country_name)
            # add the list of values from this row to the list of new data
            new_data.append(row)
    # find the length (number of rows) of new_data    
    num_new = len(new_ids)
    # if we have found new dates to process
    if num_new:
        # insert new data into the carto table
        logging.info('Adding {} new records'.format(num_new))
        cartosql.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA.keys(), CARTO_SCHEMA.values(), new_data, user=CARTO_USER, key=CARTO_KEY)

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
    logging.info('Fetching new data')
    num_new = processData(SOURCE_URL, existing_ids)
    logging.info('Previous rows: {},  New rows: {}'.format(len(existing_ids), num_new))

    # Delete data to get back to MAX_ROWS
    logging.info('Deleting excess rows')
    num_deleted = deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    # Update Resource Watch
    updateResourceWatch(num_new)

    logging.info("SUCCESS")
